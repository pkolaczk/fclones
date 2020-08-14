use core::cmp;
use std::ffi::{OsStr, OsString};

use itertools::Itertools;
use sysinfo::{DiskExt, DiskType, System, SystemExt};

use crate::files::FileLen;
use crate::path::Path;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub struct DiskDevice {
    pub index: usize,
    pub name: OsString,
    pub disk_type: DiskType,
    pub thread_pool: ThreadPool,
}

impl DiskDevice {
    fn new(index: usize, name: OsString, disk_type: DiskType) -> DiskDevice {
        let parallelism: usize = match disk_type {
            DiskType::SSD => 0, // == number of cores
            DiskType::HDD => 1,
            DiskType::Unknown(_) => 1,
        };
        DiskDevice {
            index,
            name,
            disk_type,
            thread_pool: ThreadPoolBuilder::default()
                .num_threads(parallelism)
                .build()
                .unwrap(),
        }
    }

    pub fn buffer_size(&self) -> usize {
        match self.disk_type {
            DiskType::SSD => 64 * 1024,
            DiskType::HDD => 256 * 1024,
            DiskType::Unknown(_) => 256 * 1024,
        }
    }

    pub fn min_prefix_len(&self) -> FileLen {
        FileLen(match self.disk_type {
            DiskType::SSD => 4 * 1024,
            DiskType::HDD => 64 * 1024,
            DiskType::Unknown(_) => 16 * 1024,
        })
    }

    pub fn max_prefix_len(&self) -> FileLen {
        FileLen(match self.disk_type {
            DiskType::SSD => 16 * 1024,
            DiskType::HDD => 512 * 1024,
            DiskType::Unknown(_) => 64 * 1024,
        })
    }

    pub fn suffix_len(&self) -> FileLen {
        self.max_prefix_len()
    }

    pub fn suffix_threshold(&self) -> FileLen {
        FileLen(match self.disk_type {
            DiskType::HDD => 64 * 1024 * 1024, // 64 MB
            DiskType::SSD => 64 * 1024,        // 64 kB
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
    /// If the device doesn't exist, adds a new device to devices vector and returns its index.
    /// If the device already exists, it returns the index of the existing device.
    fn add_device(&mut self, name: OsString, disk_type: DiskType) -> usize {
        if let Some((index, _)) = self.devices.iter().find_position(|d| d.name == name) {
            index
        } else {
            let index = self.devices.len();
            self.devices.push(DiskDevice::new(index, name, disk_type));
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
    pub fn new() -> DiskDevices {
        let mut sys = System::new_all();
        sys.refresh_disks();
        let mut result = DiskDevices {
            devices: Vec::new(),
            mount_points: Vec::new(),
        };

        // Default device used when we don't find any real device
        result.add_device(OsString::from("default"), DiskType::Unknown(-1));

        for d in sys.get_disks() {
            let device_name = Self::parent_device_name(d.get_name());
            let index = result.add_device(device_name, d.get_type());
            result
                .mount_points
                .push((Path::from(d.get_mount_point()), index));
        }
        result
            .mount_points
            .sort_by_key(|(p, _)| cmp::Reverse(p.component_count()));
        result
    }

    /// Returns the disk device which holds the given path.
    pub fn get_by_path(&self, path: &Path) -> &DiskDevice {
        self.mount_points
            .iter()
            .find(|(p, _)| p.is_prefix_of(path))
            .map(|&(_, index)| &self.devices[index])
            .unwrap_or(&self.devices[0])
    }

    /// Returns references to the thread pools associated with devices.
    /// The order is the same as device order, so thread pools can be selected by
    /// the device index.
    pub fn thread_pools(&self) -> Vec<&ThreadPool> {
        self.devices.iter().map(|d| &d.thread_pool).collect()
    }
}

impl Default for DiskDevices {
    fn default() -> Self {
        Self::new()
    }
}
