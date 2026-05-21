//! Duplicate directory grouping based on a previously generated file report.

use std::collections::{HashMap, HashSet};
use std::fs::{self, DirEntry, FileType};
use std::hash::Hasher;
use std::io::{self, BufWriter, Write};
use std::path::Component;
use std::sync::{mpsc, Arc};
use std::thread;

use fallible_iterator::FallibleIterator;
use metrohash::MetroHash128;
use rayon::prelude::*;

use crate::config::GroupConfig;
use crate::device::DiskDevices;
use crate::error::Error;
use crate::file::{FileHash, FileLen};
use crate::group::FileGroup;
use crate::log::{Log, LogExt, ProgressBarLength};
use crate::path::Path;
use crate::progress::ProgressTracker;
use crate::report::{GroupIterator, ReportHeader};

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileIdentity {
    file_len: FileLen,
    file_hash: FileHash,
}

type FileIdentityId = usize;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct DirSignature(u128);

#[derive(Clone, Debug)]
struct DirCandidate {
    path: Path,
    signature: DirSignature,
    total_size: FileLen,
    file_count: usize,
    ignored: IgnoredEntries,
}

#[derive(Clone, Debug)]
struct DirGroup {
    total_size: FileLen,
    file_count: usize,
    paths: Vec<Path>,
    ignored: Vec<(Path, IgnoredEntries)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DirScanResult {
    complete: bool,
    signature: DirSignature,
    total_size: FileLen,
    file_count: usize,
    ignored: IgnoredEntries,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct IgnoredEntries {
    symlinks: usize,
    empty_files: usize,
}

impl IgnoredEntries {
    fn is_empty(self) -> bool {
        self.symlinks == 0 && self.empty_files == 0
    }

    fn add(&mut self, other: IgnoredEntries) {
        self.symlinks += other.symlinks;
        self.empty_files += other.empty_files;
    }
}

#[derive(Clone, Debug)]
enum SignatureEntry {
    File {
        name: Vec<u8>,
        identity_id: FileIdentityId,
    },
    Dir {
        name: Vec<u8>,
        signature: DirSignature,
        total_size: FileLen,
        file_count: usize,
    },
}

impl SignatureEntry {
    fn file(name: Vec<u8>, identity_id: FileIdentityId) -> SignatureEntry {
        SignatureEntry::File { name, identity_id }
    }

    fn dir(name: Vec<u8>, result: DirScanResult) -> SignatureEntry {
        SignatureEntry::Dir {
            name,
            signature: result.signature,
            total_size: result.total_size,
            file_count: result.file_count,
        }
    }

    fn name(&self) -> &[u8] {
        match self {
            SignatureEntry::File { name, .. } => name,
            SignatureEntry::Dir { name, .. } => name,
        }
    }

    fn tag(&self) -> u8 {
        match self {
            SignatureEntry::File { .. } => b'F',
            SignatureEntry::Dir { .. } => b'D',
        }
    }

    fn total_size(&self, identities: &[FileIdentity]) -> FileLen {
        match self {
            SignatureEntry::File { identity_id, .. } => identities[*identity_id].file_len,
            SignatureEntry::Dir { total_size, .. } => *total_size,
        }
    }

    fn file_count(&self) -> usize {
        match self {
            SignatureEntry::File { .. } => 1,
            SignatureEntry::Dir { file_count, .. } => *file_count,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DirBucketKey {
    signature: DirSignature,
    total_size: FileLen,
    file_count: usize,
}

type ReportDirNodeId = usize;

struct ReportDirNode {
    path: Path,
    path_hash: u128,
    name: Vec<u8>,
    parent: Option<ReportDirNodeId>,
    files: Vec<SignatureEntry>,
    children: Vec<ReportDirNodeId>,
    summary: Option<DirScanResult>,
    candidate: bool,
}

struct ReportDatabase {
    identities: Vec<FileIdentity>,
    file_identity_ids: HashMap<u128, FileIdentityId>,
    dirs: Vec<ReportDirNode>,
    dir_by_hash: HashMap<u128, ReportDirNodeId>,
    candidate_paths: Vec<Path>,
    candidate_node_ids: Vec<ReportDirNodeId>,
}

struct ReportTreeBuilder<'a> {
    roots: &'a [Path],
    root_ids_by_hash: HashMap<u128, Vec<usize>>,
    base_dir: Arc<Path>,
    identities: Vec<FileIdentity>,
    file_identity_ids: HashMap<u128, FileIdentityId>,
    dirs: Vec<ReportDirNode>,
    dir_by_hash: HashMap<u128, ReportDirNodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryType {
    File,
    Dir,
    SymLink,
    Other,
}

impl EntryType {
    fn from(file_type: FileType) -> EntryType {
        if file_type.is_symlink() {
            EntryType::SymLink
        } else if file_type.is_file() {
            EntryType::File
        } else if file_type.is_dir() {
            EntryType::Dir
        } else {
            EntryType::Other
        }
    }
}

struct EntryScan {
    complete: bool,
    entry: Option<SignatureEntry>,
    ignored: IgnoredEntries,
}

struct DirVerifier<'a> {
    report: &'a ReportDatabase,
    recorded_candidates: HashSet<u128>,
    candidates: Vec<DirCandidate>,
    log: &'a dyn Log,
}

/// Returns an error when a report-producing `fclones group` configuration is not a
/// strict duplicate-file identity database suitable for directory grouping.
pub fn validate_dirgroup_source_config(config: &GroupConfig) -> Result<(), Error> {
    if config.stdin {
        return Err(Error::from(
            "Cannot infer scan roots from a report produced with fclones group --stdin",
        ));
    }
    if config.transform.is_some() {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group --transform; \
             the report describes transformed output, not on-disk file bytes",
        ));
    }
    if config.skip_content_hash {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group \
             --skip-content-hash; the report may contain false-positive file matches",
        ));
    }
    if config.unique {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group --unique; \
             the report is not a duplicate-file identity database",
        ));
    }
    if config.rf_under.is_some() {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group --rf-under; \
             the report is not a duplicate-file identity database",
        ));
    }
    if config.rf_over.unwrap_or(1) != 1 {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group --rf-over \
             other than 1; lower-replication duplicate files may be absent from the report",
        ));
    }
    if config.isolate {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group --isolate; \
             the report can suppress duplicates within one root",
        ));
    }
    if config.follow_links {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group --follow-links; \
             directory groups compare paths without following symbolic links",
        ));
    }
    if config.symbolic_links {
        return Err(Error::from(
            "Cannot use fclones dirgroup with a report produced by fclones group \
             --symbolic-links; directory groups ignore symbolic links",
        ));
    }
    Ok(())
}

/// Aggregates duplicate files from a `fclones group` report into duplicate directory groups.
pub fn dirgroup(
    group_config: GroupConfig,
    header: ReportHeader,
    mut groups: Box<GroupIterator>,
    out: impl Write + Send,
    source: &str,
    log: &dyn Log,
) -> Result<(), Error> {
    validate_dirgroup_source_config(&group_config)?;
    let roots = recover_dirgroup_roots(&group_config)?;
    warn_about_partial_report(&group_config, log);

    let group_count = header.stats.as_ref().map(|s| s.group_count as u64);
    let progress = log.progress_bar(
        "Reading report",
        group_count
            .map(ProgressBarLength::Items)
            .unwrap_or(ProgressBarLength::Unknown),
    );

    let capacity = header
        .stats
        .as_ref()
        .map(|s| s.total_file_count)
        .unwrap_or_default();
    let mut builder = ReportTreeBuilder::new(&roots, &header.base_dir, capacity);
    while let Some(group) = groups
        .next()
        .map_err(|e| Error::new(format!("Input error: {e}")))?
    {
        builder.add_group(group)?;
        progress.inc(1);
    }
    let report = builder.finish()?;
    log.info(format!(
        "Selected {} directory candidates from the source report",
        report.candidate_paths.len()
    ));

    let verified_candidates = verify_candidates(&report, &group_config, log);
    log.info(format!(
        "Verified {} complete directory candidates",
        verified_candidates.len()
    ));

    let groups = build_final_groups(verified_candidates);
    write_dirgroup_report(out, source, &roots, &groups)
        .map_err(|e| Error::new(format!("Output error: {e}")))?;
    log.info(format!("Found {} duplicate directory groups", groups.len()));
    Ok(())
}

pub fn recover_dirgroup_roots(config: &GroupConfig) -> Result<Vec<Path>, Error> {
    if config.stdin {
        return Err(Error::from(
            "Cannot infer scan roots from a report produced with fclones group --stdin",
        ));
    }

    let mut roots = Vec::new();
    for root in config.input_paths() {
        let metadata = fs::metadata(root.to_path_buf()).map_err(|e| {
            Error::new(format!(
                "Cannot access recovered scan root {}: {}",
                root.display(),
                e
            ))
        })?;
        if !metadata.is_dir() {
            return Err(Error::new(format!(
                "Recovered scan root {} is not a directory. fclones dirgroup only supports \
                 directory roots.",
                root.display()
            )));
        }
        let root = dunce::canonicalize(root.to_path_buf())
            .map(Path::from)
            .map_err(|e| Error::new(format!("Cannot normalize root {}: {}", root.display(), e)))?;
        roots.push(root);
    }

    if roots.is_empty() {
        return Err(Error::from(
            "No directory roots recovered from the source report",
        ));
    }
    Ok(prune_nested_roots(roots))
}

fn prune_nested_roots(mut roots: Vec<Path>) -> Vec<Path> {
    roots.sort_by(|a, b| {
        a.component_count()
            .cmp(&b.component_count())
            .then_with(|| a.cmp(b))
    });

    let mut pruned: Vec<Path> = Vec::with_capacity(roots.len());
    'roots: for root in roots {
        for kept in &pruned {
            if kept.is_prefix_of(&root) {
                continue 'roots;
            }
        }
        pruned.push(root);
    }
    pruned.sort();
    pruned
}

fn warn_about_partial_report(config: &GroupConfig, log: &dyn Log) {
    if !config.hidden {
        log.warn(
            "The source report was produced without --hidden; hidden files can make directory \
             candidates incomplete.",
        );
    }
    if !config.no_ignore {
        log.warn(
            "The source report was produced without --no-ignore; ignored files can make \
             directory candidates incomplete.",
        );
    }
    if config.depth.is_some() {
        log.warn(
            "The source report was produced with --depth; depth-excluded files can make \
             directory candidates incomplete.",
        );
    }
    if !config.name_patterns.is_empty() {
        log.warn(
            "The source report was produced with --name filters; name-excluded files can make \
             directory candidates incomplete.",
        );
    }
    if !config.path_patterns.is_empty() {
        log.warn(
            "The source report was produced with --path filters; path-excluded files can make \
             directory candidates incomplete.",
        );
    }
    if !config.exclude_patterns.is_empty() {
        log.warn(
            "The source report was produced with --exclude filters; excluded files can make \
             directory candidates incomplete.",
        );
    }
    let has_patterns = !config.name_patterns.is_empty()
        || !config.path_patterns.is_empty()
        || !config.exclude_patterns.is_empty();
    if has_patterns && config.ignore_case {
        log.warn(
            "The source report used --ignore-case with filters; filtered files can make \
             directory candidates incomplete.",
        );
    }
    if has_patterns && config.regex {
        log.warn(
            "The source report used --regex with filters; filtered files can make directory \
             candidates incomplete.",
        );
    }
    if config.min_size != FileLen(1) {
        log.warn(
            "The source report was produced with --min; smaller non-empty files can make \
             directory candidates incomplete.",
        );
    }
    if config.max_size.is_some() {
        log.warn(
            "The source report was produced with --max; larger files can make directory \
             candidates incomplete.",
        );
    }
    if config.one_fs {
        log.warn(
            "The source report was produced with --one-fs; files on nested filesystems can make \
             directory candidates incomplete.",
        );
    }
    if !config.match_links {
        log.warn(
            "The source report was produced without --match-links; hardlink-only duplicates can \
             be missing from the report.",
        );
    }
}

impl<'a> ReportTreeBuilder<'a> {
    fn new(roots: &'a [Path], base_dir: &Path, capacity: usize) -> ReportTreeBuilder<'a> {
        ReportTreeBuilder {
            roots,
            root_ids_by_hash: index_roots(roots),
            base_dir: Arc::new(base_dir.clone()),
            identities: Vec::with_capacity(capacity / 2),
            file_identity_ids: HashMap::with_capacity(capacity),
            dirs: Vec::with_capacity(capacity / 4),
            dir_by_hash: HashMap::with_capacity(capacity / 4),
        }
    }

    fn add_group(&mut self, group: FileGroup<Path>) -> Result<(), Error> {
        if group.file_len == FileLen(0) {
            return Ok(());
        }
        let identity = FileIdentity {
            file_len: group.file_len,
            file_hash: group.file_hash,
        };
        let identity_id = self.add_identity(identity);
        for path in group.files {
            let path = self.normalize_report_path(path)?;
            self.add_file(path, identity_id)?;
        }
        Ok(())
    }

    fn add_identity(&mut self, identity: FileIdentity) -> FileIdentityId {
        let id = self.identities.len();
        self.identities.push(identity);
        id
    }

    fn normalize_report_path(&self, path: Path) -> Result<Path, Error> {
        let path = self.base_dir.resolve(path);
        validate_normalized_report_path(&path)?;
        Ok(path)
    }

    fn add_file(&mut self, path: Path, identity_id: FileIdentityId) -> Result<(), Error> {
        let root = self.root_for_path(&path).cloned().ok_or_else(|| {
            Error::new(format!(
                "Reported path {} is outside every recovered directory root. The source report \
                 must contain only paths under the recovered roots.",
                path.display()
            ))
        })?;
        if path == root {
            return Err(Error::new(format!(
                "Reported file path {} matches a recovered directory root",
                path.display()
            )));
        }

        if let Some(previous) = self.file_identity_ids.insert(path.hash128(), identity_id) {
            if self.identities[previous] == self.identities[identity_id] {
                return Ok(());
            } else {
                return Err(Error::new(format!(
                    "Conflicting identities for reported path {}",
                    path.display()
                )));
            }
        }

        let parent = path.parent().map(|p| p.as_ref().clone()).ok_or_else(|| {
            Error::new(format!(
                "Reported path {} has no parent directory",
                path.display()
            ))
        })?;
        let name = path_file_name_bytes(&path)?;
        let parent_id = self.ensure_dir(parent, &root)?;
        self.dirs[parent_id]
            .files
            .push(SignatureEntry::file(name, identity_id));
        Ok(())
    }

    fn ensure_dir(&mut self, path: Path, root: &Path) -> Result<ReportDirNodeId, Error> {
        let path_hash = path.hash128();
        if let Some(&id) = self.dir_by_hash.get(&path_hash) {
            if self.dirs[id].path != path {
                return Err(Error::new(format!(
                    "Conflicting directory path hash for {}",
                    path.display()
                )));
            }
            return Ok(id);
        }

        let parent = if &path == root {
            None
        } else {
            let parent = path.parent().map(|p| p.as_ref().clone()).ok_or_else(|| {
                Error::new(format!(
                    "Reported directory path {} has no parent",
                    path.display()
                ))
            })?;
            Some(self.ensure_dir(parent, root)?)
        };

        let id = self.dirs.len();
        let name = path_file_name_bytes(&path).unwrap_or_default();
        self.dirs.push(ReportDirNode {
            path: path.clone(),
            path_hash,
            name,
            parent,
            files: Vec::new(),
            children: Vec::new(),
            summary: None,
            candidate: false,
        });
        self.dir_by_hash.insert(path_hash, id);
        if let Some(parent) = parent {
            self.dirs[parent].children.push(id);
        }
        Ok(id)
    }

    fn root_for_path(&self, path: &Path) -> Option<&'a Path> {
        let mut current = Some(path);
        while let Some(candidate) = current {
            if let Some(root_ids) = self.root_ids_by_hash.get(&candidate.hash128()) {
                if let Some(root_id) = root_ids
                    .iter()
                    .copied()
                    .find(|&root_id| self.roots[root_id].eq(candidate))
                {
                    return Some(&self.roots[root_id]);
                }
            }
            current = candidate.parent().map(|p| p.as_ref());
        }
        None
    }

    fn finish(mut self) -> Result<ReportDatabase, Error> {
        let buckets = self.compute_report_summaries();
        let mut candidate_paths = Vec::new();
        let mut candidate_node_ids = Vec::new();
        for ids in buckets.into_values().filter(|ids| ids.len() >= 2) {
            for id in ids {
                self.dirs[id].candidate = true;
                candidate_node_ids.push(id);
                candidate_paths.push(self.dirs[id].path.clone());
            }
        }
        candidate_paths.sort();
        candidate_node_ids.sort_by(|a, b| self.dirs[*a].path.cmp(&self.dirs[*b].path));
        Ok(ReportDatabase {
            identities: self.identities,
            file_identity_ids: self.file_identity_ids,
            dirs: self.dirs,
            dir_by_hash: self.dir_by_hash,
            candidate_paths,
            candidate_node_ids,
        })
    }

    fn compute_report_summaries(&mut self) -> HashMap<DirBucketKey, Vec<ReportDirNodeId>> {
        for id in (0..self.dirs.len()).rev() {
            let mut entries = self.dirs[id].files.clone();
            for child in self.dirs[id].children.clone() {
                let child_summary = self.dirs[child].summary.unwrap();
                if child_summary.file_count > 0 {
                    entries.push(SignatureEntry::dir(
                        self.dirs[child].name.clone(),
                        child_summary,
                    ));
                }
            }
            let summary = summarize_directory(entries, true, &self.identities);
            self.dirs[id].summary = Some(summary);
        }

        let mut buckets: HashMap<DirBucketKey, Vec<ReportDirNodeId>> = HashMap::new();
        for (id, node) in self.dirs.iter().enumerate() {
            let summary = node.summary.unwrap();
            if summary.file_count == 0 {
                continue;
            }
            buckets
                .entry(DirBucketKey {
                    signature: summary.signature,
                    total_size: summary.total_size,
                    file_count: summary.file_count,
                })
                .or_default()
                .push(id);
        }
        buckets
    }
}

fn index_roots(roots: &[Path]) -> HashMap<u128, Vec<usize>> {
    let mut root_ids_by_hash: HashMap<u128, Vec<usize>> = HashMap::with_capacity(roots.len());
    for (id, root) in roots.iter().enumerate() {
        root_ids_by_hash.entry(root.hash128()).or_default().push(id);
    }
    root_ids_by_hash
}

fn validate_normalized_report_path(path: &Path) -> Result<(), Error> {
    if path
        .to_path_buf()
        .components()
        .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
    {
        return Err(Error::new(format!(
            "Reported path {} is not normalized",
            path.display()
        )));
    }
    Ok(())
}

fn verify_candidates(
    report: &ReportDatabase,
    group_config: &GroupConfig,
    log: &dyn Log,
) -> Vec<DirCandidate> {
    if report.candidate_paths.is_empty() {
        return vec![];
    }
    let topmost = topmost_candidate_paths(report);
    let topmost_count = topmost.len();
    let devices = DiskDevices::new(&group_config.thread_pool_sizes());
    let paths_by_device = partition_candidate_paths_by_device(topmost, &devices);
    let progress = log.progress_bar(
        "Verifying directories",
        ProgressBarLength::Items(topmost_count as u64),
    );

    let (tx, rx) = mpsc::channel();
    thread::scope(|scope| {
        for (paths, device) in paths_by_device.into_iter().zip(devices.iter()) {
            if paths.is_empty() {
                continue;
            }

            let tx = tx.clone();
            let progress = progress.clone();
            scope.spawn(move || {
                let candidates = device
                    .rand_thread_pool()
                    .install(|| verify_candidate_paths(report, paths, log, progress));
                tx.send(candidates).unwrap();
            });
        }
    });
    drop(tx);

    let candidates = rx.into_iter().flatten().collect();
    deduplicate_candidates(candidates)
}

fn partition_candidate_paths_by_device(paths: Vec<Path>, devices: &DiskDevices) -> Vec<Vec<Path>> {
    let mut paths_by_device = vec![Vec::new(); devices.len()];
    for path in paths {
        let device = devices.get_by_path(&path);
        paths_by_device[device.index].push(path);
    }
    paths_by_device
}

fn verify_candidate_paths(
    report: &ReportDatabase,
    paths: Vec<Path>,
    log: &dyn Log,
    progress: Arc<dyn ProgressTracker>,
) -> Vec<DirCandidate> {
    paths
        .into_par_iter()
        .map(|path| {
            let mut verifier = DirVerifier {
                report,
                recorded_candidates: HashSet::new(),
                candidates: Vec::new(),
                log,
            };

            verifier.scan_dir(&path);
            progress.inc(1);
            verifier.candidates
        })
        .flatten()
        .collect()
}

fn deduplicate_candidates(mut candidates: Vec<DirCandidate>) -> Vec<DirCandidate> {
    let mut seen = HashSet::new();
    candidates.retain(|candidate| seen.insert(candidate.path.clone()));
    candidates
}

fn topmost_candidate_paths(report: &ReportDatabase) -> Vec<Path> {
    report
        .candidate_node_ids
        .iter()
        .filter(|&&id| !has_candidate_ancestor(id, &report.dirs))
        .map(|&id| report.dirs[id].path.clone())
        .collect()
}

fn has_candidate_ancestor(id: ReportDirNodeId, dirs: &[ReportDirNode]) -> bool {
    let mut current = dirs[id].parent;
    while let Some(id) = current {
        if dirs[id].candidate {
            return true;
        }
        current = dirs[id].parent;
    }
    false
}

impl<'a> DirVerifier<'a> {
    fn scan_dir(&mut self, path: &Path) -> DirScanResult {
        let mut complete = true;
        let mut entries = Vec::new();
        let mut ignored = IgnoredEntries::default();

        let Some(dir_entries) = self.read_entries(path, &mut complete) else {
            let result = summarize_directory(entries, false, &self.report.identities);
            self.record_candidate(path, result);
            return result;
        };

        let parent = Arc::new(path.clone());
        for dir_entry in dir_entries {
            let dir_entry = match dir_entry {
                Ok(dir_entry) => dir_entry,
                Err(e) => {
                    complete = false;
                    self.log.warn(format!(
                        "Failed to read directory entry under {}: {}",
                        path.display(),
                        e
                    ));
                    continue;
                }
            };
            let child_path = parent.join(Path::from(dir_entry.file_name()));
            let entry_scan = if complete {
                self.scan_dir_entry(&child_path, &dir_entry)
            } else {
                self.scan_after_incomplete(&child_path)
            };
            complete &= entry_scan.complete;
            ignored.add(entry_scan.ignored);
            if let Some(entry) = entry_scan.entry {
                entries.push(entry);
            }
        }

        let mut result = summarize_directory(entries, complete, &self.report.identities);
        result.ignored = ignored;
        self.record_candidate(path, result);
        result
    }

    fn read_entries(&self, path: &Path, complete: &mut bool) -> Option<fs::ReadDir> {
        Some(match fs::read_dir(path.to_path_buf()) {
            Ok(read_dir) => read_dir,
            Err(e) => {
                *complete = false;
                self.log.warn(format!(
                    "Failed to read directory {}: {}",
                    path.display(),
                    e
                ));
                return None;
            }
        })
    }

    fn scan_dir_entry(&mut self, path: &Path, dir_entry: &DirEntry) -> EntryScan {
        match dir_entry.file_type() {
            Ok(file_type) => match EntryType::from(file_type) {
                EntryType::File => self.scan_file(path),
                EntryType::Dir => self.scan_child_dir(path),
                EntryType::SymLink => self.scan_link(),
                EntryType::Other => {
                    self.log.warn(format!(
                        "Unsupported filesystem entry {} makes containing directory incomplete",
                        path.display()
                    ));
                    EntryScan {
                        complete: false,
                        entry: None,
                        ignored: IgnoredEntries::default(),
                    }
                }
            },
            Err(e) => {
                self.log.warn(format!(
                    "Failed to determine entry type of {}: {}",
                    path.display(),
                    e
                ));
                EntryScan {
                    complete: false,
                    entry: None,
                    ignored: IgnoredEntries::default(),
                }
            }
        }
    }

    fn scan_after_incomplete(&mut self, path: &Path) -> EntryScan {
        if self.report_dir_id(path).is_some() {
            let ignored = self.scan_dir(path).ignored;
            EntryScan {
                complete: true,
                entry: None,
                ignored,
            }
        } else {
            EntryScan {
                complete: true,
                entry: None,
                ignored: IgnoredEntries::default(),
            }
        }
    }

    fn scan_child_dir(&mut self, path: &Path) -> EntryScan {
        if self.report_dir_id(path).is_none() {
            let scan = self.scan_unreported_dir(path);
            return EntryScan {
                complete: scan.complete,
                entry: None,
                ignored: scan.ignored,
            };
        }

        let child = self.scan_dir(path);
        let mut scan = EntryScan {
            complete: child.complete,
            entry: None,
            ignored: child.ignored,
        };
        if child.file_count > 0 {
            scan.entry = Some(SignatureEntry::dir(
                path_file_name_bytes(path).unwrap(),
                child,
            ));
        }
        scan
    }

    fn scan_unreported_dir(&mut self, path: &Path) -> EntryScan {
        let read_dir = match fs::read_dir(path.to_path_buf()) {
            Ok(read_dir) => read_dir,
            Err(e) => {
                self.log.warn(format!(
                    "Failed to read directory {}: {}",
                    path.display(),
                    e
                ));
                return EntryScan {
                    complete: false,
                    entry: None,
                    ignored: IgnoredEntries::default(),
                };
            }
        };

        let parent = Arc::new(path.clone());
        let mut ignored = IgnoredEntries::default();
        for entry in read_dir {
            let dir_entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    self.log.warn(format!(
                        "Failed to read directory entry under {}: {}",
                        path.display(),
                        e
                    ));
                    return EntryScan {
                        complete: false,
                        entry: None,
                        ignored,
                    };
                }
            };
            let child_path = parent.join(Path::from(dir_entry.file_name()));
            let scan = self.scan_unreported_entry(&child_path, &dir_entry);
            ignored.add(scan.ignored);
            if !scan.complete || scan.entry.is_some() {
                return EntryScan {
                    complete: false,
                    entry: None,
                    ignored,
                };
            }
        }
        EntryScan {
            complete: true,
            entry: None,
            ignored,
        }
    }

    fn scan_unreported_entry(&mut self, path: &Path, dir_entry: &DirEntry) -> EntryScan {
        match dir_entry.file_type() {
            Ok(file_type) => match EntryType::from(file_type) {
                EntryType::File => self.scan_file(path),
                EntryType::Dir => {
                    if self.report_dir_id(path).is_some() {
                        let child = self.scan_dir(path);
                        EntryScan {
                            complete: child.complete && child.file_count == 0,
                            entry: None,
                            ignored: child.ignored,
                        }
                    } else {
                        self.scan_unreported_dir(path)
                    }
                }
                EntryType::SymLink => self.scan_link(),
                EntryType::Other => {
                    self.log.warn(format!(
                        "Unsupported filesystem entry {} makes containing directory incomplete",
                        path.display()
                    ));
                    EntryScan {
                        complete: false,
                        entry: None,
                        ignored: IgnoredEntries::default(),
                    }
                }
            },
            Err(e) => {
                self.log.warn(format!(
                    "Failed to determine entry type of {}: {}",
                    path.display(),
                    e
                ));
                EntryScan {
                    complete: false,
                    entry: None,
                    ignored: IgnoredEntries::default(),
                }
            }
        }
    }

    fn scan_file(&self, path: &Path) -> EntryScan {
        if let Some(&identity_id) = self.report.file_identity_ids.get(&path.hash128()) {
            let identity = &self.report.identities[identity_id];
            if identity.file_len == FileLen(0) {
                return EntryScan {
                    complete: true,
                    entry: None,
                    ignored: IgnoredEntries::default(),
                };
            }
            return EntryScan {
                complete: true,
                entry: Some(SignatureEntry::file(
                    path_file_name_bytes(path).unwrap(),
                    identity_id,
                )),
                ignored: IgnoredEntries::default(),
            };
        }

        match fs::metadata(path.to_path_buf()) {
            Ok(metadata) if metadata.is_file() && metadata.len() == 0 => EntryScan {
                complete: true,
                entry: None,
                ignored: IgnoredEntries {
                    empty_files: 1,
                    ..IgnoredEntries::default()
                },
            },
            Ok(metadata) if metadata.is_file() => EntryScan {
                complete: false,
                entry: None,
                ignored: IgnoredEntries::default(),
            },
            Ok(_) => {
                self.log.warn(format!(
                    "Unsupported filesystem entry {} makes containing directory incomplete",
                    path.display()
                ));
                EntryScan {
                    complete: false,
                    entry: None,
                    ignored: IgnoredEntries::default(),
                }
            }
            Err(e) => {
                self.log.warn(format!(
                    "Failed to read metadata of {}: {}",
                    path.display(),
                    e
                ));
                EntryScan {
                    complete: false,
                    entry: None,
                    ignored: IgnoredEntries::default(),
                }
            }
        }
    }

    fn scan_link(&self) -> EntryScan {
        EntryScan {
            complete: true,
            entry: None,
            ignored: IgnoredEntries {
                symlinks: 1,
                ..IgnoredEntries::default()
            },
        }
    }

    fn record_candidate(&mut self, path: &Path, result: DirScanResult) {
        let Some(node_id) = self.report_dir_id(path) else {
            return;
        };
        let node = &self.report.dirs[node_id];
        if result.complete
            && result.file_count > 0
            && node.candidate
            && self.recorded_candidates.insert(node.path_hash)
        {
            self.candidates.push(DirCandidate {
                path: path.clone(),
                signature: result.signature,
                total_size: result.total_size,
                file_count: result.file_count,
                ignored: result.ignored,
            });
        }
    }

    fn report_dir_id(&self, path: &Path) -> Option<ReportDirNodeId> {
        let path_hash = path.hash128();
        let id = *self.report.dir_by_hash.get(&path_hash)?;
        self.report.dirs[id].path.eq(path).then_some(id)
    }
}

fn summarize_directory(
    mut entries: Vec<SignatureEntry>,
    complete: bool,
    identities: &[FileIdentity],
) -> DirScanResult {
    entries.sort_by(|a, b| a.name().cmp(b.name()).then_with(|| a.tag().cmp(&b.tag())));

    let mut hasher = MetroHash128::new();
    let mut total_size = FileLen(0);
    let mut file_count = 0;
    for entry in &entries {
        hash_entry(&mut hasher, entry, identities);
        total_size += entry.total_size(identities);
        file_count += entry.file_count();
    }
    let (a, b) = hasher.finish128();
    DirScanResult {
        complete,
        signature: DirSignature(((a as u128) << 64) | (b as u128)),
        total_size,
        file_count,
        ignored: IgnoredEntries::default(),
    }
}

fn hash_entry(hasher: &mut MetroHash128, entry: &SignatureEntry, identities: &[FileIdentity]) {
    hasher.write_u8(entry.tag());
    hash_bytes(hasher, entry.name());
    match entry {
        SignatureEntry::File { identity_id, .. } => {
            let identity = &identities[*identity_id];
            hasher.write_u64(identity.file_len.0);
            hash_bytes(hasher, identity.file_hash.as_bytes());
        }
        SignatureEntry::Dir {
            signature,
            total_size,
            file_count,
            ..
        } => {
            hasher.write(&signature.0.to_le_bytes());
            hasher.write_u64(total_size.0);
            hasher.write_usize(*file_count);
        }
    }
}

fn hash_bytes(hasher: &mut MetroHash128, bytes: &[u8]) {
    hasher.write_usize(bytes.len());
    hasher.write(bytes);
}

fn path_file_name_bytes(path: &Path) -> Result<Vec<u8>, Error> {
    path.file_name_cstr()
        .map(|name| name.to_bytes().to_vec())
        .ok_or_else(|| Error::new(format!("Path {} has no file name", path.display())))
}

fn build_final_groups(candidates: Vec<DirCandidate>) -> Vec<DirGroup> {
    let mut buckets: HashMap<DirBucketKey, Vec<DirCandidate>> = HashMap::new();
    for candidate in candidates {
        buckets
            .entry(DirBucketKey {
                signature: candidate.signature,
                total_size: candidate.total_size,
                file_count: candidate.file_count,
            })
            .or_default()
            .push(candidate);
    }

    let mut groups: Vec<DirGroup> = buckets
        .into_values()
        .filter(|candidates| candidates.len() >= 2)
        .map(|mut candidates| {
            candidates.sort_by(|a, b| a.path.cmp(&b.path));
            let ignored = candidates
                .iter()
                .filter(|candidate| !candidate.ignored.is_empty())
                .map(|candidate| (candidate.path.clone(), candidate.ignored))
                .collect();
            DirGroup {
                total_size: candidates[0].total_size,
                file_count: candidates[0].file_count,
                ignored,
                paths: candidates.into_iter().map(|c| c.path).collect(),
            }
        })
        .collect();

    groups.sort_by(|a, b| {
        b.total_size
            .cmp(&a.total_size)
            .then_with(|| b.paths.len().cmp(&a.paths.len()))
            .then_with(|| a.paths[0].cmp(&b.paths[0]))
    });
    suppress_nested_groups(groups)
}

fn suppress_nested_groups(groups: Vec<DirGroup>) -> Vec<DirGroup> {
    // Maximal-first output: once every path in a group is nested under any
    // already-reported duplicate directory, suppress it. This is intentionally
    // not exhaustive; rerun after handling larger groups to find remaining
    // duplicate substructures.
    let mut kept_path_hashes = HashSet::new();
    let mut kept = Vec::new();
    for group in groups {
        if group
            .paths
            .iter()
            .all(|path| has_kept_strict_ancestor(path, &kept_path_hashes))
        {
            continue;
        }
        for path in &group.paths {
            kept_path_hashes.insert(path.hash128());
        }
        kept.push(group);
    }
    kept
}

fn has_kept_strict_ancestor(path: &Path, kept_path_hashes: &HashSet<u128>) -> bool {
    let mut current = path.parent().map(|p| p.as_ref().clone());
    while let Some(path) = current {
        if kept_path_hashes.contains(&path.hash128()) {
            return true;
        }
        current = path.parent().map(|p| p.as_ref().clone());
    }
    false
}

fn write_dirgroup_report(
    out: impl Write,
    source: &str,
    roots: &[Path],
    groups: &[DirGroup],
) -> io::Result<()> {
    let mut out = BufWriter::new(out);
    writeln!(out, "# Report by fclones dirgroup")?;
    writeln!(out, "# Source: {source}")?;
    writeln!(out, "# Root: {}", format_roots(roots))?;
    writeln!(out, "# Duplicate folder groups: {}", groups.len())?;
    writeln!(out, "#")?;
    writeln!(
        out,
        "# Directories are duplicates when all files are duplicates and no extra files are present (excludes empty files, empty directories, and symlinks)."
    )?;
    writeln!(
        out,
        "# Unreadable or unsupported entries make a directory incomplete."
    )?;
    writeln!(out)?;

    for group in groups {
        writeln!(
            out,
            "{} ({} B) * {}, {} files:",
            group.total_size,
            group.total_size.0,
            group.paths.len(),
            group.file_count
        )?;
        for path in &group.paths {
            writeln!(out, "    {}", path.to_escaped_string())?;
        }
        write_ignored_entries(&mut out, group)?;
    }
    out.flush()
}

fn write_ignored_entries(out: &mut impl Write, group: &DirGroup) -> io::Result<()> {
    if group.ignored.is_empty() {
        return Ok(());
    }

    writeln!(out, "# Ignored: {}", ignored_group_label(&group.ignored))?;
    for (path, ignored) in &group.ignored {
        writeln!(
            out,
            "#   {}: {}",
            path.to_escaped_string(),
            format_ignored_counts(*ignored)
        )?;
    }
    Ok(())
}

fn ignored_group_label(ignored: &[(Path, IgnoredEntries)]) -> &'static str {
    let has_symlinks = ignored.iter().any(|(_, ignored)| ignored.symlinks > 0);
    let has_empty_files = ignored.iter().any(|(_, ignored)| ignored.empty_files > 0);
    match (has_symlinks, has_empty_files) {
        (true, true) => "symlinks and empty files",
        (true, false) => "symlinks",
        (false, true) => "empty files",
        (false, false) => "nothing",
    }
}

fn format_ignored_counts(ignored: IgnoredEntries) -> String {
    let mut parts = Vec::with_capacity(2);
    if ignored.symlinks > 0 {
        parts.push(format_count(ignored.symlinks, "symlink", "symlinks"));
    }
    if ignored.empty_files > 0 {
        parts.push(format_count(
            ignored.empty_files,
            "empty file",
            "empty files",
        ));
    }
    parts.join(", ")
}

fn format_count(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("{count} {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn format_roots(roots: &[Path]) -> String {
    roots
        .iter()
        .map(Path::to_escaped_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod test {
    use std::fs::{self, create_dir_all};
    use std::io;
    use std::sync::{Arc, Mutex};

    use crate::config::GroupConfig;
    use crate::file::{FileHash, FileLen};
    use crate::group::FileGroup;
    use crate::log::{Log, LogLevel, ProgressBarLength};
    use crate::progress::{NoProgressBar, ProgressTracker};
    use crate::report::{FileStats, ReportHeader};
    use crate::util::test::{with_dir, write_file};
    use crate::TIMESTAMP_FMT;

    use super::*;

    #[derive(Default)]
    struct TestLog {
        warnings: Mutex<Vec<String>>,
    }

    impl Log for TestLog {
        fn progress_bar(&self, _msg: &str, _len: ProgressBarLength) -> Arc<dyn ProgressTracker> {
            Arc::new(NoProgressBar)
        }

        fn log(&self, level: LogLevel, msg: String) {
            if matches!(level, LogLevel::Warn) {
                self.warnings.lock().unwrap().push(msg);
            }
        }
    }

    #[derive(Default)]
    struct TestSignatures {
        identities: Vec<FileIdentity>,
    }

    impl TestSignatures {
        fn file(&mut self, name: &str, len: u64, hash: u128) -> SignatureEntry {
            let identity_id = self.identities.len();
            self.identities.push(FileIdentity {
                file_len: FileLen(len),
                file_hash: FileHash::from(hash),
            });
            SignatureEntry::file(name.as_bytes().to_vec(), identity_id)
        }

        fn dir(&self, name: &str, entries: Vec<SignatureEntry>) -> SignatureEntry {
            SignatureEntry::dir(name.as_bytes().to_vec(), self.summarize(entries))
        }

        fn summarize(&self, entries: Vec<SignatureEntry>) -> DirScanResult {
            summarize_directory(entries, true, &self.identities)
        }
    }

    #[test]
    fn test_prune_nested_roots_removes_nested_and_keeps_siblings() {
        let roots = vec![
            Path::from("/a/b"),
            Path::from("/a"),
            Path::from("/c"),
            Path::from("/a"),
            Path::from("/ab"),
        ];
        assert_eq!(
            prune_nested_roots(roots),
            vec![Path::from("/a"), Path::from("/ab"), Path::from("/c")]
        );
    }

    #[test]
    fn test_identical_directory_inputs_produce_identical_signatures() {
        let mut signatures = TestSignatures::default();
        let a_file = signatures.file("a.txt", 1, 1);
        let a_child_file = signatures.file("b.txt", 2, 2);
        let a_child = signatures.dir("child", vec![a_child_file]);
        let a = signatures.summarize(vec![a_file, a_child]);

        let b_child_file = signatures.file("b.txt", 2, 2);
        let b_child = signatures.dir("child", vec![b_child_file]);
        let b_file = signatures.file("a.txt", 1, 1);
        let b = signatures.summarize(vec![b_child, b_file]);

        assert_eq!(a.signature, b.signature);
        assert_eq!(a.total_size, FileLen(3));
        assert_eq!(a.file_count, 2);
    }

    #[test]
    fn test_different_directory_inputs_produce_different_signatures() {
        let mut signatures = TestSignatures::default();
        let base_file = signatures.file("a.txt", 1, 1);
        let base = signatures.summarize(vec![base_file]);
        let different_name_file = signatures.file("b.txt", 1, 1);
        let different_name = signatures.summarize(vec![different_name_file]);
        let different_size_file = signatures.file("a.txt", 2, 1);
        let different_size = signatures.summarize(vec![different_size_file]);
        let different_hash_file = signatures.file("a.txt", 1, 2);
        let different_hash = signatures.summarize(vec![different_hash_file]);
        let different_child_file = signatures.file("a.txt", 1, 1);
        let different_child_dir = signatures.dir("child", vec![different_child_file]);
        let different_child = signatures.summarize(vec![different_child_dir]);

        assert_ne!(base.signature, different_name.signature);
        assert_ne!(base.signature, different_size.signature);
        assert_ne!(base.signature, different_hash.signature);
        assert_ne!(base.signature, different_child.signature);
    }

    #[test]
    fn test_topmost_candidate_selection_removes_nested_starts() {
        let a = Path::from("/r/a");
        let ab = Path::from("/r/a/b");
        let c = Path::from("/r/c");
        let dirs = vec![
            report_node(a.clone(), None, true),
            report_node(ab, Some(0), true),
            report_node(c.clone(), None, true),
        ];
        let report = ReportDatabase {
            identities: Vec::new(),
            file_identity_ids: HashMap::new(),
            dir_by_hash: dirs
                .iter()
                .enumerate()
                .map(|(id, dir)| (dir.path_hash, id))
                .collect(),
            dirs,
            candidate_paths: vec![a.clone(), c.clone()],
            candidate_node_ids: vec![0, 1, 2],
        };

        assert_eq!(topmost_candidate_paths(&report), vec![a, c]);
    }

    #[test]
    fn test_deduplicate_candidates_keeps_one_per_path() {
        let a = Path::from("/r/a");
        let b = Path::from("/r/b");
        let candidates = vec![
            dir_candidate(a.clone(), 1),
            dir_candidate(b.clone(), 1),
            dir_candidate(a.clone(), 1),
        ];

        let candidates = deduplicate_candidates(candidates);

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].path, a);
        assert_eq!(candidates[1].path, b);
    }

    #[test]
    fn test_report_tree_builder_selects_duplicate_directory_candidates() {
        let roots = vec![Path::from("/r")];
        let mut builder = ReportTreeBuilder::new(&roots, &Path::from("/"), 2);
        builder
            .add_group(FileGroup {
                file_len: FileLen(4),
                file_hash: FileHash::from(1),
                files: vec![Path::from("/r/a/file.txt"), Path::from("/r/b/file.txt")],
            })
            .unwrap();

        let report = builder.finish().unwrap();

        assert_eq!(
            report.candidate_paths,
            vec![Path::from("/r/a"), Path::from("/r/b")]
        );
        assert_eq!(report.identities.len(), 1);
        assert_eq!(report.file_identity_ids.len(), 2);
        assert_eq!(report.candidate_node_ids.len(), 2);
        assert!(report
            .dir_by_hash
            .contains_key(&Path::from("/r/a").hash128()));
    }

    #[test]
    fn test_root_for_path_uses_indexed_ancestor_lookup() {
        let roots = vec![Path::from("/a"), Path::from("/b")];
        let builder = ReportTreeBuilder::new(&roots, &Path::from("/"), 0);

        assert_eq!(
            builder.root_for_path(&Path::from("/b/c/file.txt")),
            Some(&roots[1])
        );
        assert_eq!(builder.root_for_path(&Path::from("/c/file.txt")), None);
    }

    #[test]
    fn test_incomplete_scan_still_visits_reported_child_candidate() {
        with_dir("dirgroup/incomplete_visits_child", |root| {
            let a_child = root.join("a").join("child");
            let b_child = root.join("b").join("child");
            create_dir_all(&a_child).unwrap();
            create_dir_all(&b_child).unwrap();
            write_file(&a_child.join("file.txt"), "same");
            write_file(&b_child.join("file.txt"), "same");

            let roots = vec![Path::from(root)];
            let mut builder = ReportTreeBuilder::new(&roots, &Path::from(root), 2);
            builder
                .add_group(FileGroup {
                    file_len: FileLen(4),
                    file_hash: FileHash::from(1),
                    files: vec![
                        Path::from(a_child.join("file.txt")),
                        Path::from(b_child.join("file.txt")),
                    ],
                })
                .unwrap();
            let report = builder.finish().unwrap();
            let log = TestLog::default();
            let mut verifier = DirVerifier {
                report: &report,
                recorded_candidates: HashSet::new(),
                candidates: Vec::new(),
                log: &log,
            };

            verifier.scan_after_incomplete(&Path::from(a_child.as_path()));

            assert_eq!(verifier.candidates.len(), 1);
            assert_eq!(verifier.candidates[0].path, Path::from(a_child.as_path()));
        });
    }

    fn dir_candidate(path: Path, signature: u128) -> DirCandidate {
        DirCandidate {
            path,
            signature: DirSignature(signature),
            total_size: FileLen(1),
            file_count: 1,
            ignored: IgnoredEntries::default(),
        }
    }

    fn report_node(path: Path, parent: Option<ReportDirNodeId>, candidate: bool) -> ReportDirNode {
        let path_hash = path.hash128();
        let name = path_file_name_bytes(&path).unwrap_or_default();
        ReportDirNode {
            path,
            path_hash,
            name,
            parent,
            files: Vec::new(),
            children: Vec::new(),
            summary: None,
            candidate,
        }
    }

    #[test]
    fn test_suppress_nested_groups_uses_strict_ancestry() {
        let groups = vec![
            DirGroup {
                total_size: FileLen(10),
                file_count: 2,
                paths: vec![Path::from("/a"), Path::from("/b")],
                ignored: vec![],
            },
            DirGroup {
                total_size: FileLen(5),
                file_count: 1,
                paths: vec![Path::from("/a/c"), Path::from("/b/c")],
                ignored: vec![],
            },
            DirGroup {
                total_size: FileLen(10),
                file_count: 2,
                paths: vec![Path::from("/a"), Path::from("/b")],
                ignored: vec![],
            },
        ];
        let kept = suppress_nested_groups(groups);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].paths, vec![Path::from("/a"), Path::from("/b")]);
        assert_eq!(kept[1].paths, vec![Path::from("/a"), Path::from("/b")]);
    }

    fn header(base_dir: Path, total_file_count: usize, group_count: usize) -> ReportHeader {
        ReportHeader {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            timestamp: chrono::DateTime::parse_from_str(
                "2021-08-27 12:11:23.456 +0000",
                TIMESTAMP_FMT,
            )
            .unwrap(),
            command: vec![],
            base_dir,
            stats: Some(FileStats {
                group_count,
                total_file_count,
                total_file_size: FileLen(0),
                redundant_file_count: total_file_count,
                redundant_file_size: FileLen(0),
                missing_file_count: 0,
                missing_file_size: FileLen(0),
            }),
        }
    }

    fn group_iter(groups: Vec<FileGroup<Path>>) -> Box<GroupIterator> {
        Box::new(fallible_iterator::convert(
            groups.into_iter().map(Ok::<_, io::Error>),
        ))
    }

    fn test_config(root: &std::path::Path) -> GroupConfig {
        GroupConfig {
            hidden: true,
            no_ignore: true,
            match_links: true,
            paths: vec![Path::from(root)],
            base_dir: Path::from(root.parent().unwrap()),
            ..Default::default()
        }
    }

    #[test]
    fn test_identical_directory_trees_produce_group() {
        with_dir("dirgroup/identical", |root| {
            let a = root.join("a");
            let b = root.join("b");
            create_dir_all(&a).unwrap();
            create_dir_all(&b).unwrap();
            write_file(&a.join("file.txt"), "same");
            write_file(&b.join("file.txt"), "same");

            let groups = vec![FileGroup {
                file_len: FileLen(4),
                file_hash: FileHash::from(1),
                files: vec![
                    Path::from(a.join("file.txt")),
                    Path::from(b.join("file.txt")),
                ],
            }];
            let mut out = Vec::new();
            dirgroup(
                test_config(root),
                header(Path::from(root), 2, 1),
                group_iter(groups),
                Box::new(&mut out),
                "<test>",
                &TestLog::default(),
            )
            .unwrap();
            let out = String::from_utf8(out).unwrap();
            assert!(out.contains("Duplicate folder groups: 1"));
            assert!(out.contains(a.to_string_lossy().as_ref()));
            assert!(out.contains(b.to_string_lossy().as_ref()));
        });
    }

    #[test]
    fn test_unreported_non_empty_file_excludes_directory() {
        with_dir("dirgroup/extra_file", |root| {
            let a = root.join("a");
            let b = root.join("b");
            create_dir_all(&a).unwrap();
            create_dir_all(&b).unwrap();
            write_file(&a.join("file.txt"), "same");
            write_file(&b.join("file.txt"), "same");
            write_file(&b.join("extra.txt"), "extra");

            let groups = vec![FileGroup {
                file_len: FileLen(4),
                file_hash: FileHash::from(1),
                files: vec![
                    Path::from(a.join("file.txt")),
                    Path::from(b.join("file.txt")),
                ],
            }];
            let mut out = Vec::new();
            dirgroup(
                test_config(root),
                header(Path::from(root), 2, 1),
                group_iter(groups),
                Box::new(&mut out),
                "<test>",
                &TestLog::default(),
            )
            .unwrap();
            let out = String::from_utf8(out).unwrap();
            assert!(out.contains("Duplicate folder groups: 0"));
        });
    }

    #[test]
    fn test_unreported_non_empty_side_subtree_excludes_directory() {
        with_dir("dirgroup/side_subtree", |root| {
            let a = root.join("a");
            let b = root.join("b");
            create_dir_all(&a).unwrap();
            create_dir_all(b.join("side")).unwrap();
            write_file(&a.join("file.txt"), "same");
            write_file(&b.join("file.txt"), "same");
            write_file(&b.join("side").join("extra.txt"), "extra");

            let groups = vec![FileGroup {
                file_len: FileLen(4),
                file_hash: FileHash::from(1),
                files: vec![
                    Path::from(a.join("file.txt")),
                    Path::from(b.join("file.txt")),
                ],
            }];
            let mut out = Vec::new();
            dirgroup(
                test_config(root),
                header(Path::from(root), 2, 1),
                group_iter(groups),
                Box::new(&mut out),
                "<test>",
                &TestLog::default(),
            )
            .unwrap();
            let out = String::from_utf8(out).unwrap();
            assert!(out.contains("Duplicate folder groups: 0"));
        });
    }

    #[test]
    fn test_rejects_non_normalized_report_path() {
        with_dir("dirgroup/non_normalized_report_path", |root| {
            let report_root = root.join("root");
            let outside = root.join("outside");
            create_dir_all(&report_root).unwrap();
            create_dir_all(outside.join("a")).unwrap();
            create_dir_all(outside.join("b")).unwrap();
            write_file(&outside.join("a").join("file.txt"), "same");
            write_file(&outside.join("b").join("file.txt"), "same");

            let groups = vec![FileGroup {
                file_len: FileLen(4),
                file_hash: FileHash::from(1),
                files: vec![
                    Path::from(report_root.join("..").join("outside/a/file.txt")),
                    Path::from(report_root.join("..").join("outside/b/file.txt")),
                ],
            }];
            let mut out = Vec::new();
            let err = dirgroup(
                test_config(&report_root),
                header(Path::from(root), 2, 1),
                group_iter(groups),
                Box::new(&mut out),
                "<test>",
                &TestLog::default(),
            )
            .unwrap_err();

            assert!(err.message.contains("is not normalized"));
        });
    }

    #[test]
    fn test_empty_files_and_empty_directories_are_ignored() {
        with_dir("dirgroup/empty_ignored", |root| {
            let a = root.join("a");
            let b = root.join("b");
            create_dir_all(a.join("empty-dir")).unwrap();
            create_dir_all(&b).unwrap();
            write_file(&a.join("file.txt"), "same");
            write_file(&b.join("file.txt"), "same");
            fs::File::create(a.join("empty.txt")).unwrap();
            fs::File::create(b.join("empty.txt")).unwrap();

            let groups = vec![FileGroup {
                file_len: FileLen(4),
                file_hash: FileHash::from(1),
                files: vec![
                    Path::from(a.join("file.txt")),
                    Path::from(b.join("file.txt")),
                ],
            }];
            let mut out = Vec::new();
            dirgroup(
                test_config(root),
                header(Path::from(root), 2, 1),
                group_iter(groups),
                Box::new(&mut out),
                "<test>",
                &TestLog::default(),
            )
            .unwrap();
            let out = String::from_utf8(out).unwrap();
            assert!(out.contains("Duplicate folder groups: 1"));
            assert!(out.contains("# Ignored: empty files"));
            assert!(out.contains(format!("#   {}: 1 empty file", a.to_string_lossy()).as_str()));
            assert!(out.contains(format!("#   {}: 1 empty file", b.to_string_lossy()).as_str()));
        });
    }

    #[test]
    #[cfg(unix)]
    fn test_symlinks_are_ignored_and_reported_per_group() {
        use std::os::unix::fs::symlink;

        with_dir("dirgroup/symlinks_ignored", |root| {
            let a = root.join("a");
            let b = root.join("b");
            create_dir_all(&a).unwrap();
            create_dir_all(&b).unwrap();
            write_file(&a.join("file.txt"), "same");
            write_file(&b.join("file.txt"), "same");
            symlink(std::path::PathBuf::from("file.txt"), a.join("link")).unwrap();
            symlink(std::path::PathBuf::from("file.txt"), b.join("link")).unwrap();
            fs::File::create(a.join("empty-a.txt")).unwrap();
            fs::File::create(a.join("empty-b.txt")).unwrap();

            let groups = vec![FileGroup {
                file_len: FileLen(4),
                file_hash: FileHash::from(1),
                files: vec![
                    Path::from(a.join("file.txt")),
                    Path::from(b.join("file.txt")),
                ],
            }];
            let mut out = Vec::new();
            dirgroup(
                test_config(root),
                header(Path::from(root), 2, 1),
                group_iter(groups),
                Box::new(&mut out),
                "<test>",
                &TestLog::default(),
            )
            .unwrap();
            let out = String::from_utf8(out).unwrap();
            assert!(out.contains("Duplicate folder groups: 1"));
            assert!(out.contains(a.to_string_lossy().as_ref()));
            assert!(out.contains(b.to_string_lossy().as_ref()));
            assert!(out.contains("# Ignored: symlinks and empty files"));
            assert!(out.contains(
                format!("#   {}: 1 symlink, 2 empty files", a.to_string_lossy()).as_str()
            ));
            assert!(out.contains(format!("#   {}: 1 symlink", b.to_string_lossy()).as_str()));
        });
    }

    #[test]
    fn test_rejects_unsafe_source_modes() {
        let mut config = GroupConfig {
            hidden: true,
            no_ignore: true,
            paths: vec![Path::from("/tmp")],
            ..Default::default()
        };

        config.transform = Some("cat".to_string());
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--transform"));
        config.transform = None;

        config.skip_content_hash = true;
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--skip-content-hash"));
        config.skip_content_hash = false;

        config.unique = true;
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--unique"));
        config.unique = false;

        config.rf_under = Some(2);
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--rf-under"));
        config.rf_under = None;

        config.rf_over = Some(2);
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--rf-over"));
        config.rf_over = None;

        config.isolate = true;
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--isolate"));
        config.isolate = false;

        config.follow_links = true;
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--follow-links"));
        config.follow_links = false;

        config.symbolic_links = true;
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--symbolic-links"));
        config.symbolic_links = false;

        config.stdin = true;
        assert!(validate_dirgroup_source_config(&config)
            .unwrap_err()
            .message
            .contains("--stdin"));
    }
}
