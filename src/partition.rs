use core::cmp;

use std_semaphore::Semaphore;
use sysinfo::{DiskExt, DiskType, System, SystemExt};

use crate::files::FileLen;
use crate::path::Path;

pub struct Partition {
    pub mount_point: Path,
    pub disk_type: DiskType,
    /// Limits the number of concurrent I/O operations sent to a single device
    pub semaphore: Semaphore,
}

impl Partition {
    fn new(mount_point: Path, disk_type: DiskType) -> Partition {
        let parallelism: isize = match disk_type {
            DiskType::HDD => 1,
            DiskType::SSD => std::isize::MAX >> 3,
            DiskType::Unknown(_) => 1,
        };
        Partition {
            mount_point,
            disk_type,
            semaphore: Semaphore::new(parallelism),
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
            DiskType::HDD => 64 * 1024,
            DiskType::SSD => 4 * 1024,
            DiskType::Unknown(_) => 16 * 1024,
        })
    }

    pub fn max_prefix_len(&self) -> FileLen {
        FileLen(match self.disk_type {
            DiskType::HDD => 512 * 1024,
            DiskType::SSD => 16 * 1024,
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

pub struct Partitions {
    system: Vec<Partition>,
    fallback: Partition,
}

impl Partitions {
    pub fn new() -> Partitions {
        let mut sys = System::new_all();
        sys.refresh_disks();
        let mut devices = Vec::new();
        for d in sys.get_disks() {
            devices.push(Partition::new(
                Path::from(d.get_mount_point()),
                d.get_type(),
            ));
        }
        devices.sort_by_key(|p| cmp::Reverse(p.mount_point.component_count()));
        Partitions {
            system: devices,
            fallback: Partition::new(Path::from("/"), DiskType::Unknown(-1)),
        }
    }

    /// Returns the disk partition which holds the given path.
    pub fn get_by_path(&self, path: &Path) -> &Partition {
        self.system
            .iter()
            .find(|&p| p.mount_point.is_prefix_of(path))
            .unwrap_or(&self.fallback)
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self::new()
    }
}
