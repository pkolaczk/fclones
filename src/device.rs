use core::cmp;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::ops::Index;

use itertools::Itertools;
use lazy_init::Lazy;
use rayon::{ThreadPool, ThreadPoolBuilder};
use sysinfo::{DiskExt, DiskType, System, SystemExt};

use crate::files::FileLen;
use crate::path::Path;

#[derive(Clone, Copy, Debug)]
pub struct Parallelism {
    pub random: usize,
    pub sequential: usize,
}

impl Parallelism {
    pub fn default_for(disk_type: DiskType) -> Parallelism {
        match disk_type {
            DiskType::SSD => Parallelism {
                random: 0,
                sequential: 0,
            }, // == number of cores
            DiskType::HDD => Parallelism {
                random: 8,
                sequential: 1,
            },
            DiskType::Removable => Parallelism {
                random: 4,
                sequential: 1,
            },
            DiskType::Unknown(_) => Parallelism {
                random: 4,
                sequential: 1,
            },
        }
    }
}

pub struct DiskDevice {
    pub index: usize,
    pub name: OsString,
    pub disk_type: DiskType,
    pub parallelism: Parallelism,
    seq_thread_pool: Lazy<ThreadPool>,
    rand_thread_pool: Lazy<ThreadPool>,
}

impl DiskDevice {
    fn new(
        index: usize,
        name: OsString,
        disk_type: DiskType,
        parallelism: Parallelism,
    ) -> DiskDevice {
        DiskDevice {
            index,
            name,
            disk_type,
            parallelism,
            seq_thread_pool: Lazy::new(),
            rand_thread_pool: Lazy::new(),
        }
    }

    fn build_thread_pool(num_threads: usize) -> ThreadPool {
        ThreadPoolBuilder::default()
            .num_threads(num_threads)
            .build()
            .unwrap()
    }

    pub fn seq_thread_pool(&self) -> &ThreadPool {
        self.seq_thread_pool
            .get_or_create(|| Self::build_thread_pool(self.parallelism.sequential))
    }

    pub fn rand_thread_pool(&self) -> &ThreadPool {
        self.rand_thread_pool
            .get_or_create(|| Self::build_thread_pool(self.parallelism.random))
    }

    pub fn buf_len(&self) -> usize {
        match self.disk_type {
            DiskType::SSD => 64 * 1024,
            DiskType::HDD => 256 * 1024,
            DiskType::Removable => 256 * 1024,
            DiskType::Unknown(_) => 256 * 1024,
        }
    }

    pub fn min_prefix_len(&self) -> FileLen {
        FileLen(match self.disk_type {
            DiskType::SSD => 4 * 1024,
            DiskType::HDD => 4 * 1024,
            DiskType::Removable => 4 * 1024,
            DiskType::Unknown(_) => 4 * 1024,
        })
    }

    pub fn max_prefix_len(&self) -> FileLen {
        FileLen(match self.disk_type {
            DiskType::SSD => 4 * 1024,
            DiskType::HDD => 16 * 1024,
            DiskType::Removable => 16 * 1024,
            DiskType::Unknown(_) => 16 * 1024,
        })
    }

    pub fn suffix_len(&self) -> FileLen {
        self.max_prefix_len()
    }

    pub fn suffix_threshold(&self) -> FileLen {
        FileLen(match self.disk_type {
            DiskType::HDD => 64 * 1024 * 1024, // 64 MB
            DiskType::SSD => 64 * 1024,        // 64 kB
            DiskType::Removable => 64 * 1024 * 1024,
            DiskType::Unknown(_) => 64 * 1024 * 1024,
        })
    }
}

/// Finds disk devices by file paths
pub struct DiskDevices {
    devices: Vec<DiskDevice>,
    mount_points: Vec<(Path, usize)>,
}

impl DiskDevices {
    #[cfg(test)]
    pub fn single(disk_type: DiskType, parallelism: usize) -> DiskDevices {
        let device = DiskDevice::new(
            0,
            OsString::from("/"),
            disk_type,
            Parallelism {
                random: parallelism,
                sequential: parallelism,
            },
        );
        DiskDevices {
            devices: vec![device],
            mount_points: vec![(Path::from("/"), 0)],
        }
    }

    /// Reads the preferred parallelism level for the device based on the
    /// device name or the device type (ssd/hdd) from `pool_sizes` map.
    /// Returns the value under the "default" key if device was not found,
    /// or 0 if "default" doesn't exist in the map.
    /// If found, the device key is removed from the map.
    fn get_parallelism(
        name: &OsStr,
        disk_type: DiskType,
        pool_sizes: &mut HashMap<OsString, Parallelism>,
    ) -> Parallelism {
        let mut dev_key = OsString::new();
        dev_key.push("dev:");
        dev_key.push(name);
        match pool_sizes.remove(&dev_key) {
            Some(p) => p,
            None => {
                let p = match disk_type {
                    DiskType::SSD => pool_sizes.get(OsStr::new("ssd")),
                    DiskType::HDD => pool_sizes.get(OsStr::new("hdd")),
                    DiskType::Removable => pool_sizes.get(OsStr::new("removable")),
                    DiskType::Unknown(_) => pool_sizes.get(OsStr::new("unknown")),
                };
                match p {
                    Some(p) => *p,
                    None => *pool_sizes
                        .get(OsStr::new("default"))
                        .unwrap_or(&Parallelism::default_for(disk_type)),
                }
            }
        }
    }

    /// If the device doesn't exist, adds a new device to devices vector and returns its index.
    /// If the device already exists, it returns the index of the existing device.
    fn add_device(
        &mut self,
        name: OsString,
        disk_type: DiskType,
        pool_sizes: &mut HashMap<OsString, Parallelism>,
    ) -> usize {
        if let Some((index, _)) = self.devices.iter().find_position(|d| d.name == name) {
            index
        } else {
            let index = self.devices.len();
            let parallelism = Self::get_parallelism(&name, disk_type, pool_sizes);
            self.devices
                .push(DiskDevice::new(index, name, disk_type, parallelism));
            index
        }
    }

    /// If `name` is a disk partition, it attempts to return the disk device name the partition
    /// resides on. Otherwise, and on failures, it just returns the same `name`.
    #[cfg(target_os = "linux")]
    fn parent_device_name(name: &OsStr) -> OsString {
        block_utils::get_parent_devpath_from_path(&std::path::Path::new(name))
            .unwrap_or(None)
            .map(|p| p.into_os_string())
            .unwrap_or_else(|| name.to_os_string())
    }

    #[cfg(not(target_os = "linux"))]
    fn parent_device_name(name: &OsStr) -> OsString {
        name.to_os_string()
    }

    /// Reads the list of partitions and disks from the system and builds the `DiskDevices`
    /// structure from that information.
    pub fn new(pool_sizes: &mut HashMap<OsString, Parallelism>) -> DiskDevices {
        let mut sys = System::new();
        sys.refresh_disks_list();
        let mut result = DiskDevices {
            devices: Vec::new(),
            mount_points: Vec::new(),
        };

        // Default device used when we don't find any real device
        result.add_device(OsString::from("default"), DiskType::Unknown(-1), pool_sizes);

        for d in sys.get_disks() {
            let device_name = Self::parent_device_name(d.get_name());
            let index = result.add_device(device_name, d.get_type(), pool_sizes);
            result
                .mount_points
                .push((Path::from(d.get_mount_point()), index));
        }
        result
            .mount_points
            .sort_by_key(|(p, _)| cmp::Reverse(p.component_count()));

        pool_sizes.remove(OsStr::new("unknown"));
        pool_sizes.remove(OsStr::new("ssd"));
        pool_sizes.remove(OsStr::new("hdd"));
        result
    }

    /// Returns the disk device which holds the given path
    pub fn get_by_path(&self, path: &Path) -> &DiskDevice {
        self.mount_points
            .iter()
            .find(|(p, _)| p.is_prefix_of(path))
            .map(|&(_, index)| &self.devices[index])
            .unwrap_or(&self.devices[0])
    }

    /// Returns the first device on the list
    pub fn get_default(&self) -> &DiskDevice {
        &self.devices[0]
    }

    /// Returns the number of devices
    pub fn len(&self) -> usize {
        self.devices.len()
    }

    /// Returns true if there are no devices
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty()
    }

    /// Returns an iterator over devices
    pub fn iter(&self) -> impl Iterator<Item = &DiskDevice> {
        self.devices.iter()
    }
}

impl Default for DiskDevices {
    fn default() -> Self {
        let mut pool_sizes = HashMap::new();
        Self::new(&mut pool_sizes)
    }
}

impl Index<usize> for DiskDevices {
    type Output = DiskDevice;

    fn index(&self, index: usize) -> &Self::Output {
        &self.devices[index]
    }
}
