//! Output formatting.

use std::io;
use std::io::{BufRead, BufReader, Error, ErrorKind, Read, Write};

use console::style;
use lazy_static::lazy_static;
use regex::Regex;
use serde::Serialize;

use crate::config::OutputFormat;
use crate::files::{FileHash, FileLen};

use crate::util::IteratorWrapper;
use crate::FileGroup;
use chrono::{DateTime, FixedOffset};
use fallible_iterator::FallibleIterator;

use std::borrow::Borrow;
use std::cell::Cell;

use crate::path::Path;
use std::cmp::min;
use std::fmt::Display;

/// Describes how many redundant files were found, in how many groups,
/// how much space can be reclaimed, etc.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct FileStats {
    pub group_count: usize,
    pub redundant_file_count: usize,
    pub redundant_file_size: FileLen,
}

/// Data in the header of the whole report.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ReportHeader {
    /// The program version that produced the report
    pub version: String,
    /// The date and time when the report was produced
    pub timestamp: DateTime<FixedOffset>,
    /// Full shell command containing arguments of the search run that produced the report
    pub command: Vec<String>,
    /// Information on the number of duplicate files reported.
    /// This is optional to allow streaming the report out before finding all files in the future.
    pub stats: Option<FileStats>,
}

/// Formats and writes duplicate files report to a stream.
/// Supports many formats: text, csv, json, etc.
pub struct ReportWriter<W: Write> {
    out: W,
    color: bool,
}

impl<W: Write> ReportWriter<W> {
    pub fn new(out: W, color: bool) -> ReportWriter<W> {
        ReportWriter { out, color }
    }

    fn write_header_line(&mut self, line: &str) -> io::Result<()> {
        writeln!(
            self.out,
            "{}",
            style(format!("# {}", line))
                .cyan()
                .force_styling(self.color)
        )
    }

    /// Writes the report in human-readable text format.
    ///
    /// A group of identical files starts with a group header at column 0,
    /// containing the size and hash of each file in the group.
    /// Then file paths are printed in separate, indented lines.
    ///
    /// # Example
    /// ```text
    /// # Report by fclones 0.12.0
    /// # Timestamp: Mon, 03 May 2021 13:22:51 +0000
    /// # Command: target/debug/fclones find . -o report.txt
    /// # Found 553 file groups
    /// # 271.8 MB in 4266 redundant files can be removed
    /// 5649a555c131508c4a757d9e14c4aea6, 6626689 B (6.6 MB) * 5:
    ///     /home/pkolaczk/Projekty/fclones/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.0/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.1/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.2/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.11.0/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta
    /// f79ce189d76620fd921986943087dc3a, 5815999 B (5.8 MB) * 5:
    ///     /home/pkolaczk/Projekty/fclones/target/debug/deps/libserde-af05e0212e5def7d.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.0/target/debug/deps/libserde-af05e0212e5def7d.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.1/target/debug/deps/libserde-af05e0212e5def7d.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.2/target/debug/deps/libserde-af05e0212e5def7d.rmeta
    ///     /home/pkolaczk/Projekty/fclones/target/package/fclones-0.11.0/target/debug/deps/libserde-af05e0212e5def7d.rmeta
    /// ```
    pub fn write_as_text<I, G, P>(&mut self, header: &ReportHeader, groups: I) -> io::Result<()>
    where
        I: IntoIterator<Item = G>,
        G: Borrow<FileGroup<P>>,
        P: Display,
    {
        let command = shell_words::join(header.command.iter());
        self.write_header_line(&format!("Report by fclones {}", header.version))?;
        self.write_header_line(&format!("Timestamp: {}", header.timestamp.to_rfc2822()))?;
        self.write_header_line(&format!("Command: {}", command))?;
        if let Some(stats) = &header.stats {
            self.write_header_line(&format!("Found {} file groups", stats.group_count))?;
            self.write_header_line(&format!(
                "{} B ({}) in {} redundant files can be removed",
                stats.redundant_file_size.0, stats.redundant_file_size, stats.redundant_file_count
            ))?;
        }

        for g in groups {
            let g = g.borrow();
            let group_header = format!(
                "{}, {} B ({}) * {}:",
                g.file_hash,
                g.file_len.0,
                g.file_len,
                g.files.len()
            );
            let group_header = style(group_header).yellow();
            writeln!(self.out, "{}", group_header.force_styling(self.color),)?;
            for f in g.files.iter() {
                writeln!(self.out, "    {}", f)?;
            }
        }
        Ok(())
    }

    /// Writes the report in `fdupes` compatible format.
    /// This is very similar to the TEXT format, but there are no headers
    /// for each group, and groups are separated with empty lines.
    pub fn write_as_fdupes<'a, I, P>(&mut self, _header: &ReportHeader, groups: I) -> io::Result<()>
    where
        I: IntoIterator<Item = &'a FileGroup<P>>,
        P: Display + 'a,
    {
        for g in groups {
            for f in g.files.iter() {
                writeln!(self.out, "{}", f)?;
            }
            writeln!(self.out)?;
        }
        Ok(())
    }

    /// Writes results in CSV format.
    ///
    /// Each file group is written as one line.
    /// The number of columns is dynamic.
    /// Columns:
    /// - file size in bytes
    /// - file hash (may be empty)
    /// - number of files in the group
    /// - file paths - each file in a separate column
    pub fn write_as_csv<I, G, P>(&mut self, _header: &ReportHeader, groups: I) -> io::Result<()>
    where
        I: IntoIterator<Item = G>,
        G: Borrow<FileGroup<P>>,
        P: Display,
    {
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(b',')
            .quote_style(csv::QuoteStyle::Necessary)
            .flexible(true)
            .from_writer(&mut self.out);

        wtr.write_record(&["size", "hash", "count", "files"])?;
        for g in groups {
            let g = g.borrow();
            let mut record = csv::StringRecord::new();
            record.push_field(g.file_len.0.to_string().as_str());
            record.push_field(g.file_hash.to_string().as_str());
            record.push_field(g.files.len().to_string().as_str());
            for f in g.files.iter() {
                record.push_field(format!("{}", f).as_ref());
            }
            wtr.write_record(&record)?;
        }
        wtr.flush()
    }

    /// Writes results as JSON.
    /// # Example output
    /// ```json
    /// {
    ///   "header": {
    ///     "version": "0.12.0",
    ///     "timestamp": "2021-05-03T13:20:59.285409824+00:00",
    ///     "command": [
    ///       "target/debug/fclones",
    ///       "find",
    ///       ".",
    ///       "-f",
    ///       "JSON",
    ///       "-o",
    ///       "report.json"
    ///     ],
    ///     "stats": {
    ///       "group_count": 553,
    ///       "redundant_file_count": 4266,
    ///       "redundant_file_size": 271838709
    ///     }
    ///   },
    ///   "groups": [
    ///     {
    ///       "file_len": 6626689,
    ///       "file_hash": "5649a555c131508c4a757d9e14c4aea6",
    ///       "files": [
    ///         "/home/pkolaczk/Projekty/fclones/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.0/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.1/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.2/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.11.0/target/debug/deps/libregex_syntax-94c84f5600b85f6e.rmeta"
    ///       ]
    ///     },
    ///     {
    ///       "file_len": 5815999,
    ///       "file_hash": "f79ce189d76620fd921986943087dc3a",
    ///       "files": [
    ///         "/home/pkolaczk/Projekty/fclones/target/debug/deps/libserde-af05e0212e5def7d.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.0/target/debug/deps/libserde-af05e0212e5def7d.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.1/target/debug/deps/libserde-af05e0212e5def7d.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.10.2/target/debug/deps/libserde-af05e0212e5def7d.rmeta",
    ///         "/home/pkolaczk/Projekty/fclones/target/package/fclones-0.11.0/target/debug/deps/libserde-af05e0212e5def7d.rmeta"
    ///       ]
    ///     },
    ///      ...
    ///   ]
    /// }
    /// ```
    pub fn write_as_json<I, P>(&mut self, header: &ReportHeader, groups: I) -> io::Result<()>
    where
        I: IntoIterator<Item = P>,
        P: Serialize,
    {
        #[derive(Serialize)]
        struct Report<'a, G: Serialize> {
            header: &'a ReportHeader,
            groups: G,
        }

        let report = Report {
            header,
            groups: IteratorWrapper(Cell::new(Some(groups))),
        };

        serde_json::to_writer_pretty(&mut self.out, &report)?;
        Ok(())
    }

    /// Writes the report in the format given by `format` parameter.
    pub fn write<I, G, P>(
        &mut self,
        format: &OutputFormat,
        header: &ReportHeader,
        groups: I,
    ) -> io::Result<()>
    where
        I: IntoIterator<Item = G>,
        G: Borrow<FileGroup<P>> + Serialize,
        P: Display + Serialize,
    {
        match format {
            OutputFormat::Text => self.write_as_text(header, groups),
            OutputFormat::Fdupes => self.write_as_json(header, groups),
            OutputFormat::Csv => self.write_as_csv(header, groups),
            OutputFormat::Json => self.write_as_json(header, groups),
        }
    }
}

/// Iterates the contents of the report.
/// Each emitted item is a group of duplicate files.
pub struct TextReportIterator<R: Read> {
    stream: BufReader<R>,
    line_buf: String,
}

/// Helper struct to encapsulate the data in the header before each group of identical files
#[derive(Debug, Eq, PartialEq, Serialize)]
struct GroupHeader {
    count: usize,
    file_len: FileLen,
    file_hash: FileHash,
}

impl<R> TextReportIterator<R>
where
    R: Read,
{
    fn read_first_non_comment_line(&mut self) -> io::Result<Option<&str>> {
        loop {
            self.line_buf.clear();
            self.stream.read_line(&mut self.line_buf)?;
            let line = &self.line_buf;
            if line.trim().is_empty() {
                return Ok(None);
            }
            if !line.starts_with('#') {
                break;
            }
        }
        Ok(Some(&self.line_buf))
    }

    fn read_group_header(&mut self) -> io::Result<Option<GroupHeader>> {
        let header_str = match self.read_first_non_comment_line()? {
            None => return Ok(None),
            Some(s) => s,
        };

        lazy_static! {
            static ref GROUP_HEADER_RE: Regex =
                Regex::new(r"^([a-f0-9]{32}), ([0-9]+) B [^*]* \* ([0-9]+):\n$").unwrap();
        }

        let captures = GROUP_HEADER_RE.captures(header_str).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Malformed group header: {}", header_str),
            )
        })?;

        Ok(Some(GroupHeader {
            file_hash: FileHash(
                u128::from_str_radix(captures.get(1).unwrap().as_str(), 16).unwrap(),
            ),
            file_len: FileLen(captures.get(2).unwrap().as_str().parse::<u64>().unwrap()),
            count: captures.get(3).unwrap().as_str().parse::<usize>().unwrap(),
        }))
    }

    fn read_paths(&mut self, count: usize) -> io::Result<Vec<Path>> {
        let mut paths = Vec::with_capacity(min(count, 1024));
        for _ in 0..count {
            self.line_buf.clear();
            let n = self.stream.read_line(&mut self.line_buf)?;
            let path_str = &self.line_buf;
            if n == 0 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Unexpected end of file.",
                ));
            }
            if !path_str.starts_with("    ") || path_str.trim().is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Path expected: {}", path_str),
                ));
            }
            paths.push(Path::from(path_str.trim()));
        }
        Ok(paths)
    }
}

impl<R: Read> FallibleIterator for TextReportIterator<R> {
    type Item = FileGroup<Path>;
    type Error = std::io::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        Ok(match self.read_group_header()? {
            Some(header) => {
                let paths = self.read_paths(header.count)?;
                Some(FileGroup {
                    file_len: header.file_len,
                    file_hash: header.file_hash,
                    files: paths,
                })
            }
            None => None,
        })
    }
}

/// Reads a text report from a stream.
///
/// Currently supports only the default text report format.
/// Does not load the whole report into memory.
/// Allows iterating over groups of files.
pub struct TextReportReader<R: Read> {
    pub header: ReportHeader,
    pub groups: TextReportIterator<R>,
}

impl<R: Read> TextReportReader<R> {
    fn read_line(stream: &mut impl BufRead) -> io::Result<String> {
        let mut line_buf = String::new();
        stream.read_line(&mut line_buf)?;
        Ok(line_buf)
    }

    fn read_extract(
        stream: &mut impl BufRead,
        regex: &Regex,
        msg: &str,
    ) -> io::Result<Vec<String>> {
        let line = Self::read_line(stream)?;
        Ok(regex
            .captures(line.trim())
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, msg.to_owned()))?
            .iter()
            .skip(1)
            .map(|c| c.unwrap().as_str().to_owned())
            .collect())
    }

    /// Creates a new reader for reading from the given stream and
    /// decodes the report header.
    /// Reports an io::Error with ErrorKind::InvalidData
    /// if the report header is malformed.
    pub fn new(input_stream: R) -> io::Result<TextReportReader<R>> {
        let mut stream = BufReader::new(input_stream);

        lazy_static! {
            static ref VERSION_RE: Regex =
                Regex::new(r"^# Report by fclones ([0-9]+\.[0-9]+\.[0-9]+)").unwrap();
            static ref TIMESTAMP_RE: Regex = Regex::new(r"^# Timestamp: (.*)").unwrap();
            static ref COMMAND_RE: Regex = Regex::new(r"^# Command: (.*)").unwrap();
            static ref GROUP_COUNT_RE: Regex =
                Regex::new(r"^# Found ([0-9+]) file groups").unwrap();
            static ref STATS_RE: Regex =
                Regex::new(r"^# ([0-9]+) B \([^)]+\) in ([0-9]+) redundant files can be removed")
                    .unwrap();
        }

        let version = Self::read_extract(
            &mut stream,
            &VERSION_RE,
            "Not a default fclones report. \
            Formats other than the default one are not supported yet.",
        )?
        .swap_remove(0);
        let timestamp = Self::read_extract(
            &mut stream,
            &TIMESTAMP_RE,
            "Malformed header: Missing timestamp",
        )?
        .swap_remove(0);
        let timestamp = DateTime::parse_from_rfc2822(&timestamp).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Malformed header: Failed to parse timestamp: {}", e),
            )
        })?;
        let command = Self::read_extract(
            &mut stream,
            &COMMAND_RE,
            "Malformed header: Missing command",
        )?
        .swap_remove(0);
        let command = shell_words::split(&command).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Malformed header: Failed to parse command arguments: {}", e),
            )
        })?;
        let group_count = Self::read_extract(
            &mut stream,
            &GROUP_COUNT_RE,
            "Malformed header: Missing group count",
        )?
        .swap_remove(0);
        let group_count: usize = group_count.parse().map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Malformed header: Failed to parse group count: {}", e),
            )
        })?;
        let stats_line = Self::read_extract(
            &mut stream,
            &STATS_RE,
            "Malformed header: Missing file statistics line",
        )?;
        let redundant_file_size = FileLen(stats_line[0].parse().map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Malformed header: Failed to parse file size {}: {}",
                    stats_line[0], e
                ),
            )
        })?);
        let redundant_file_count: usize = stats_line[1].parse().map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Malformed header: Failed to parse file count {}: {}",
                    stats_line[1], e
                ),
            )
        })?;

        let header = ReportHeader {
            version,
            timestamp,
            command,
            stats: Some(FileStats {
                group_count,
                redundant_file_count,
                redundant_file_size,
            }),
        };
        Ok(TextReportReader {
            header,
            groups: TextReportIterator {
                stream,
                line_buf: String::new(),
            },
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::files::{FileHash, FileLen};
    use crate::path::Path;

    use chrono::Utc;
    use tempfile::NamedTempFile;

    fn dummy_report_header() -> ReportHeader {
        ReportHeader {
            command: vec!["fclones".to_owned(), "find".to_owned(), ".".to_owned()],
            version: env!("CARGO_PKG_VERSION").to_owned(),
            timestamp: DateTime::from(Utc::now()),
            stats: Some(FileStats {
                group_count: 4,
                redundant_file_count: 234,
                redundant_file_size: FileLen(1000),
            }),
        }
    }

    #[test]
    fn test_text_report_reader_reads_header() {
        let header = dummy_report_header();
        let groups: Vec<FileGroup<Path>> = vec![];

        let output = NamedTempFile::new().unwrap();
        let input = output.reopen().unwrap();

        let mut writer = ReportWriter::new(output, false);
        writer.write_as_text(&header, groups.into_iter()).unwrap();

        let reader = TextReportReader::new(input).unwrap();
        assert_eq!(reader.header.version, header.version);
        assert_eq!(reader.header.command, header.command);
        assert_eq!(
            reader.header.timestamp.timestamp(),
            header.timestamp.timestamp()
        );
        assert_eq!(reader.header.stats, header.stats);
    }

    #[test]
    fn test_text_report_reader_reads_files() {
        let header = dummy_report_header();
        let groups = vec![
            FileGroup {
                file_len: FileLen(100),
                file_hash: FileHash(0x00112233445566778899aabbccddeeff),
                files: vec![Path::from("a"), Path::from("b")],
            },
            FileGroup {
                file_len: FileLen(40),
                file_hash: FileHash(0x0000000000000555555555ffffffffff),
                files: vec![Path::from("c"), Path::from("d")],
            },
        ];

        let output = NamedTempFile::new().unwrap();
        let input = output.reopen().unwrap();

        let mut writer = ReportWriter::new(output, false);
        writer.write_as_text(&header, groups.iter()).unwrap();
        let reader = TextReportReader::new(input).unwrap();

        let groups2: Vec<_> = reader.groups.collect().unwrap();
        assert_eq!(groups, groups2);
    }
}
