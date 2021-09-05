use core::cmp;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::ops::Index;

use itertools::Itertools;
use lazy_init::Lazy;
use rayon::{ThreadPool, ThreadPoolBuilder};
use regex::Regex;
use sysinfo::{DiskExt, DiskType, System, SystemExt};

use crate::config::Parallelism;
use crate::files::FileLen;
use crate::path::Path;

impl Parallelism {
    pub fn default_for(disk_type: DiskType) -> Parallelism {
        let cpu_count = num_cpus::get();
        match disk_type {
            // SSDs typically benefit from a lot of parallelism.
            // Some users will probably want to increase it even more.
            DiskType::SSD => Parallelism {
                random: 4 * cpu_count,
                sequential: 4 * cpu_count,
            },
            // Rotational drives can't serve multiple requests at once.
            // After introducing access ordering in fclones 0.9.0 it turns out
            // that we get slightly more IOPS when we schedule random access operations on a
            // single thread. For sequential scanning of big files, parallel access can hurt a lot,
            // so 1 is the only possible choice here.
            DiskType::HDD => Parallelism {
                random: 1,
                sequential: 1,
            },
            // Unknown device here, so we need to stay away from potentially extremely bad defaults.
            // If the underlying device is an SSD, a single-threaded mode can
            // degrade random I/O performance many times. On the other hand making parallel random
            // access to a HDD didn't degrade performance by more than 30% in our tests,
            // and sometimes it can speed things up.
            // For sequential reads of big files we obviously stay single threaded,
            // as multithreading can hurt really a lot in case the underlying device is rotational.
            _ => Parallelism {
                random: 4 * cpu_count,
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
        pool_sizes: &HashMap<OsString, Parallelism>,
    ) -> Parallelism {
        let mut dev_key = OsString::new();
        dev_key.push("dev:");
        dev_key.push(name);
        match pool_sizes.get(&dev_key) {
            Some(p) => *p,
            None => {
                let p = match disk_type {
                    DiskType::SSD => pool_sizes.get(OsStr::new("ssd")),
                    DiskType::HDD => pool_sizes.get(OsStr::new("hdd")),
                    DiskType::Removable => pool_sizes.get(OsStr::new("removable")),
                    DiskType::Unknown(_) => pool_sizes.get(OsStr::new("unknown")),
                };
                match p {
                    Some(p) => *p,
                    None => pool_sizes
                        .get(OsStr::new("default"))
                        .copied()
                        .unwrap_or_else(|| Parallelism::default_for(disk_type)),
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
        pool_sizes: &HashMap<OsString, Parallelism>,
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
    fn physical_device_name(name: &OsStr) -> OsString {
        let regex = Regex::new(r"^/dev/([fhs]d[a-z]|nvme[0-9]+).*").unwrap();
        let name_str = name.to_string_lossy();
        match regex.captures(name_str.as_ref()) {
            Some(captures) => {
                let parent = "/dev/".to_owned() + captures.get(1).unwrap().as_str();
                OsString::from(parent)
            }
            None => name.to_os_string(),
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn parent_device_name(name: &OsStr) -> OsString {
        name.to_os_string()
    }

    /// Reads the list of partitions and disks from the system and builds the `DiskDevices`
    /// structure from that information.
    pub fn new(pool_sizes: &HashMap<OsString, Parallelism>) -> DiskDevices {
        let mut sys = System::new();
        sys.refresh_disks_list();
        let mut result = DiskDevices {
            devices: Vec::new(),
            mount_points: Vec::new(),
        };

        // Default device used when we don't find any real device
        result.add_device(OsString::from("default"), DiskType::Unknown(-1), pool_sizes);

        for d in sys.get_disks() {
            let device_name = Self::physical_device_name(d.get_name());
            let index = result.add_device(device_name, d.get_type(), pool_sizes);
            result
                .mount_points
                .push((Path::from(d.get_mount_point()), index));
        }
        result
            .mount_points
            .sort_by_key(|(p, _)| cmp::Reverse(p.component_count()));

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

    /// Returns the disk device by its device name (not mount point)
    pub fn get_by_name(&self, name: &OsStr) -> Option<&DiskDevice> {
        self.devices.iter().find(|&d| d.name == name)
    }

    /// Returns the first device on the list
    pub fn get_default(&self) -> &DiskDevice {
        &self.devices[0]
    }

    /// Returns the number of devices
    pub fn len(&self) -> usize {
        self.devices.len()
    }

    /// Returns an iterator over devices
    pub fn iter(&self) -> impl Iterator<Item = &DiskDevice> {
        self.devices.iter()
    }

    /// Returns device_group identifiers recognized by the constructor
    pub fn device_types() -> Vec<&'static str> {
        vec!["ssd", "hdd", "removable", "unknown"]
    }
}

impl Default for DiskDevices {
    fn default() -> Self {
        let pool_sizes = HashMap::new();
        Self::new(&pool_sizes)
    }
}

impl Index<usize> for DiskDevices {
    type Output = DiskDevice;

    fn index(&self, index: usize) -> &Self::Output {
        &self.devices[index]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_physical_device_name() {
        assert_eq!(
            DiskDevices::physical_device_name(OsStr::new("/dev/sda")),
            OsString::from("/dev/sda")
        );
        assert_eq!(
            DiskDevices::physical_device_name(OsStr::new("/dev/sda1")),
            OsString::from("/dev/sda")
        );
        assert_eq!(
            DiskDevices::physical_device_name(OsStr::new("/dev/hdc20")),
            OsString::from("/dev/hdc")
        );
        assert_eq!(
            DiskDevices::physical_device_name(OsStr::new("/dev/nvme0n1p3")),
            OsString::from("/dev/nvme0")
        );
        assert_eq!(
            DiskDevices::physical_device_name(OsStr::new("/dev/unknown")),
            OsString::from("/dev/unknown")
        );
    }
}
