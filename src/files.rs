use std::fs::File;
use std::hash::Hasher;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use fasthash::{city::crc::Hasher128, FastHasher, HasherExt};
use std::cmp::min;

/// Returns file size in bytes
pub fn file_len(file: &PathBuf) -> u64 {
    match std::fs::metadata(file) {
        Ok(metadata) => metadata.len(),
        Err(e) => { eprintln!("Failed to read size of {}: {}", file.display(), e); 0 }
    }
}

/// Computes hash of initial `len` bytes of a file.
/// If the file does not exist or is not readable, it prints an error to stderr and returns `None`.
///
/// # Example
/// ```
/// use dff::files::file_hash;
/// use std::path::PathBuf;
/// // Assume test1.dat and test2.dat contents differ and are longer than 16 bytes
/// let hash1 = file_hash(&PathBuf::from("test/test1.dat"), std::u64::MAX).unwrap();
/// let hash2 = file_hash(&PathBuf::from("test/test2.dat"), std::u64::MAX).unwrap();
/// let hash3 = file_hash(&PathBuf::from("test/test2.dat"), 16).unwrap();
/// assert_ne!(hash1, hash2);
/// assert_ne!(hash2, hash3);
/// ```
pub fn file_hash(path: &PathBuf, len: u64) -> Option<u128> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Failed to open file {}: {}", path.display(), e);
            return None;
        }
    };
    let mut count: u64 = 0;
    let mut reader = BufReader::with_capacity(4096, file);
    let mut hasher = Hasher128::new();
    while count < len {
        match reader.fill_buf() {
            Ok(&[]) => break,
            Ok(buf) => {
                let to_read = len - count;
                let length = buf.len() as u64;
                let actual_read = min(length, to_read) as usize;
                count += actual_read as u64;
                hasher.write(&buf[0..actual_read]);
                reader.consume(actual_read);
            },
            Err(e) => {
                eprintln!("Error reading file {}: {}", path.display(), e);
                return None;
            }
        }
    }
    Some(hasher.finish_ext())
}
