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
use std::fmt::Display;

#[derive(Serialize)]
pub struct FileStats {
    pub group_count: usize,
    pub redundant_file_count: usize,
    pub redundant_file_size: FileLen,
}

#[derive(Serialize)]
pub struct ReportHeader {
    pub version: String,
    pub timestamp: DateTime<FixedOffset>,
    pub command: Vec<String>,
    pub stats: Option<FileStats>,
}

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
                "{} in {} redundant files can be removed",
                stats.redundant_file_size, stats.redundant_file_count
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

pub struct TextReportIterator<R: Read> {
    stream: BufReader<R>,
}

pub struct TextReportReader<R: Read> {
    header: ReportHeader,
    groups: TextReportIterator<R>,
}

impl<R: Read> TextReportReader<R> {
    fn read_extract(stream: &mut impl BufRead, regex: &Regex, msg: &str) -> io::Result<String> {
        let mut line_buf = String::new();
        stream.read_line(&mut line_buf)?;
        Ok(regex
            .captures(&line_buf.trim())
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, msg.to_owned()))?
            .get(1)
            .unwrap()
            .as_str()
            .to_owned())
    }

    pub fn new(input_stream: R) -> io::Result<TextReportReader<R>> {
        lazy_static! {
            static ref VERSION_RE: Regex =
                Regex::new(r"^# Report by fclones ([0-9]+\.[0-9]+\.[0-9]+)$").unwrap();
            static ref TIMESTAMP_RE: Regex = Regex::new(r"^# Timestamp: (.*)$").unwrap();
            static ref COMMAND_RE: Regex = Regex::new(r"^# Command: (fclones.*)$").unwrap();
            static ref GROUP_COUNT_RE: Regex = Regex::new(r"# Found ([0-9+]) groups").unwrap();
        }

        let mut stream = BufReader::new(input_stream);
        let version = Self::read_extract(
            &mut stream,
            &VERSION_RE,
            "Not a default fclones report. \
            Formats other than the default one are not supported yet.",
        )?;
        let timestamp = Self::read_extract(
            &mut stream,
            &TIMESTAMP_RE,
            "Malformed header: Missing timestamp",
        )?;
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
        )?;
        let command = shell_words::split(&command).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Malformed header: Failed to parse command arguments: {}", e),
            )
        })?;

        // let config: Config = Config::from_iter_safe(&command).map_err(|e| {
        //     let message: String = e.message;
        //     let message: String = message.chars().take_while(|&c| c != '\n').collect();
        //     Error::new(
        //         ErrorKind::InvalidData,
        //         format!("Malformed header: Incorrect fclones command: {}", message),
        //     )
        // })?;

        let header = ReportHeader {
            version,
            timestamp,
            command,
            stats: None,
        };
        Ok(TextReportReader {
            header,
            groups: TextReportIterator { stream },
        })
    }
}

//
impl<R: Read> FallibleIterator for TextReportIterator<R> {
    type Item = FileGroup<Path>;
    type Error = std::io::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        lazy_static! {
            static ref GROUP_HEADER_RE: Regex =
                Regex::new(r"^([a-f0-9]{32}), ([0-9]+) B [^*]* \* ([0-9]+):\n$").unwrap();
        }

        let mut line = String::new();
        loop {
            self.stream.read_line(&mut line)?;
            if line.trim().is_empty() {
                return Ok(None);
            }
            if !line.starts_with('#') {
                break;
            }
        }

        let captures = GROUP_HEADER_RE.captures(&line).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                format!("Malformed group header: {}", &line),
            )
        })?;

        let file_hash = u128::from_str_radix(captures.get(1).unwrap().as_str(), 16).unwrap();
        let file_len = captures.get(2).unwrap().as_str().parse::<u64>().unwrap();
        let count = captures.get(3).unwrap().as_str().parse::<usize>().unwrap();
        println!("count: {}", count);
        let mut paths = Vec::with_capacity(count);
        for _ in 0..count {
            line.clear();
            let n = self.stream.read_line(&mut line)?;
            if n == 0 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Unexpected end of file.",
                ));
            }
            if !line.starts_with("    ") || line.trim().is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Path expected: {}", line),
                ));
            }
            paths.push(Path::from(line.trim()));
        }

        Ok(Some(FileGroup {
            file_len: FileLen(file_len),
            file_hash: FileHash(file_hash),
            files: paths,
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::files::{FileHash, FileLen};
    use crate::path::Path;

    use chrono::Utc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_text_report_reader_reads_header() {
        let header = ReportHeader {
            command: vec!["fclones".to_owned(), "find".to_owned(), ".".to_owned()],
            version: env!("CARGO_PKG_VERSION").to_owned(),
            timestamp: DateTime::from(Utc::now()),
            stats: None,
        };
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
    }

    #[test]
    fn test_text_report_reader_reads_files() {
        let header = ReportHeader {
            command: vec!["fclones".to_owned(), "find".to_owned(), ".".to_owned()],
            version: env!("CARGO_PKG_VERSION").to_owned(),
            timestamp: DateTime::from(Utc::now()),
            stats: None,
        };

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
