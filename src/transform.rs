use std::cell::RefCell;
use std::ffi::OsString;
use std::fs::{create_dir_all, remove_dir_all, File};
use std::io;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{none_of, one_of};
use nom::combinator::map;
use nom::error::{ErrorKind, ParseError};
use nom::multi::{many1, separated_list};
use nom::sequence::tuple;
use nom::IResult;
use regex::Regex;
use uuid::Uuid;

use crate::files::{stream_hash, FileHash, FileLen};
use crate::log::Log;
use crate::path::Path;

/// Keeps the results of the transform program execution
pub struct Output {
    output_len: FileLen,
    output_hash: FileHash,
    status: ExitStatus,
    stderr: String,
}

/// Controls how we pass data to the child process.
/// By default, the file to process is sent to the standard input of the child process.
/// Some programs do not accept reading input from the stdin, but prefer to be pointed
/// to a file by a command-line option - in this case `Named` variant is used.
enum InputConf {
    StdIn(PathBuf),
    Named(PathBuf),
}

/// Controls how we read data out from the child process.
/// By default we read output directly from the standard output of the child process.
/// If the preprocessor program can't output data to its stdout, but supports only writing
/// to files, it can be configured to write to a named pipe, and we read from that named pipe.
enum OutputConf {
    /// Pipe data directly to StdOut
    StdOut,
    /// Send data through a named pipe
    Named(PathBuf),
}

/// Transforms files through an external program.
/// The `command_str` field contains a path to a program and its space separated arguments.
/// The command takes a file given in the `$IN` variable and produces an `$OUT` file.
pub struct Transform {
    pub command_str: String,
    pub program: String,
    pub tmp_dir: PathBuf,
}

impl Transform {
    pub fn new(command_str: String) -> io::Result<Transform> {
        let has_out = RefCell::new(false);
        let parsed = Self::parse_command(&command_str, |s: &str| match s {
            "OUT" if cfg!(windows) => {
                *has_out.borrow_mut() = true;
                OsString::from("$OUT")
            }
            _ => OsString::from(s),
        });

        if cfg!(windows) && has_out.into_inner() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "$OUT not supported on Windows yet",
            ));
        }

        let program = match parsed.first() {
            Some(p) => p.clone().into_string().unwrap(),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Command cannot be empty",
                ))
            }
        };

        // Check if the program is runnable, fail fast if it is not.
        match Command::new(&program).spawn() {
            Ok(mut child) => {
                let _ignore = child.kill();
            }
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("Cannot launch {}: {}", program, e),
                ))
            }
        }

        Ok(Transform {
            command_str,
            program,
            tmp_dir: Transform::create_temp_dir()?,
        })
    }

    /// Creates the directory where preprocessed files will be stored
    fn create_temp_dir() -> io::Result<PathBuf> {
        let tmp = std::env::temp_dir().join(format!("fclones-{:032x}", Uuid::new_v4().as_u128()));
        match create_dir_all(&tmp) {
            Ok(()) => Ok(tmp),
            Err(e) => Err(io::Error::new(
                e.kind(),
                format!(
                    "Failed to create temporary directory {}: {}",
                    tmp.display(),
                    e
                ),
            )),
        }
    }

    /// Returns the output file path for the given input file path
    pub fn output(&self, input: &Path) -> PathBuf {
        self.tmp_dir.join(format!("{:x}", input.hash128()))
    }

    /// Processes the file and logs any errors with the given logger.
    /// The standard output and error streams of the child process are captured
    /// and logged in case of a non-zero exit code.
    pub fn run_or_log_err(&self, input: &Path, log: &Log) -> Option<(FileLen, FileHash)> {
        let result = self.run(input);
        match result {
            Err(e) => {
                log.err(format!("Failed to execute {}: {}", self.program, e));
                None
            }
            Ok(result) if !result.status.success() => {
                log.err(format!(
                    "Failed to transform {}: {} returned non-zero status code: {}{}",
                    input,
                    self.program,
                    result.status.code().unwrap(),
                    Self::format_output_stream(result.stderr.as_str(), "STDERR")
                ));
                None
            }
            Ok(result) => Some((result.output_len, result.output_hash)),
        }
    }

    fn format_output_stream(output: &str, name: &str) -> String {
        let output = output.trim().to_string();
        if output.is_empty() {
            output
        } else {
            format!(
                "\n------------------------- {} -------------------------\n{}\
                 \n----------------------------------------------------------",
                name, output
            )
        }
    }

    /// Processes the input file, and computes the length and hash of the returned output stream
    pub fn run(&self, input: &Path) -> io::Result<Output> {
        let (args, input_conf, output_conf) = self.make_args(&input);
        let mut command = Self::build_command(&args, &input_conf, &output_conf)?;
        Self::execute(&mut command, &output_conf)
    }

    /// Creates arguments, input and output configuration for processing given input path.
    /// The first element of the argument vector contains the program name.
    fn make_args(&self, input: &Path) -> (Vec<OsString>, InputConf, OutputConf) {
        let input_conf = RefCell::<InputConf>::new(InputConf::StdIn(input.to_path_buf()));
        let output_conf = RefCell::<OutputConf>::new(OutputConf::StdOut);

        let args = Self::parse_command(self.command_str.as_str(), |arg| match arg {
            "IN" => {
                let input = input.to_path_buf();
                input_conf.replace(InputConf::Named(input.clone()));
                input.into_os_string()
            }
            "OUT" => {
                let output = self.output(input);
                output_conf.replace(OutputConf::Named(output.clone()));
                output.into_os_string()
            }
            _ => OsString::from(arg),
        });

        (args, input_conf.into_inner(), output_conf.into_inner())
    }

    /// Builds the `Command` struct from the parsed arguments
    fn build_command(
        args: &[OsString],
        input_conf: &InputConf,
        output_conf: &OutputConf,
    ) -> io::Result<Command> {
        let mut args = args.iter();
        let mut command = Command::new(args.next().unwrap());
        command.args(args);
        command.stderr(Stdio::piped());

        if let InputConf::StdIn(input_path) = input_conf {
            command.stdin(File::open(&input_path)?);
        } else {
            command.stdin(Stdio::null());
        }

        if let OutputConf::Named(output) = output_conf {
            command.stdout(Stdio::null());
            Self::create_named_pipe(&output)?;
        } else {
            command.stdout(Stdio::piped());
        }

        Ok(command)
    }

    #[cfg(unix)]
    fn create_named_pipe(path: &PathBuf) -> io::Result<()> {
        use nix::sys::stat;
        use nix::unistd::mkfifo;
        if let Err(e) = mkfifo(path, stat::Mode::S_IRWXU) {
            return Err(io::Error::new(
                io::Error::from(e.as_errno().unwrap()).kind(),
                format!("Failed to create named pipe {}: {}", path.display(), e),
            ));
        }
        Ok(())
    }

    #[cfg(windows)]
    fn create_named_pipe(_path: &PathBuf) -> io::Result<()> {
        unimplemented!()
    }

    /// Spawns the command process,
    /// computes its output length and its hash and captures its standard error into a string.
    /// Blocks until the child process terminates.
    fn execute(command: &mut Command, output_conf: &OutputConf) -> io::Result<Output> {
        let mut child = command.spawn()?;

        // We call 'take' to avoid borrowing `child` for longer than a single line.
        // We can't reference stdout/stderr directly, because a mutable borrow of a field
        // creates a mutable borrow of the containing struct, but later we need to mutably
        // borrow `child` again to wait on it.
        let child_out = child.stdout.take();
        let child_err = child.stderr.take();

        // Capture the stderr in background in order to avoid a deadlock when the child process
        // would block on writing to stdout, and this process would block on reading stderr
        // (or the other way round).
        // The other solution could be to use non-blocking I/O, but threads look simpler.
        let stderr_reaper = std::thread::spawn(move || {
            let mut str = String::new();
            let _ = child_err.unwrap().read_to_string(&mut str);
            str
        });

        let result = if let OutputConf::Named(output) = output_conf {
            stream_hash(&mut File::open(output)?, FileLen::MAX, |_| {})
        } else {
            stream_hash(&mut child_out.unwrap(), FileLen::MAX, |_| {})
        }?;

        Ok(Output {
            output_len: result.0,
            output_hash: result.1,
            status: child.wait()?,
            stderr: stderr_reaper.join().unwrap(),
        })
    }

    /// Compares the input with a regular expression and returns the first match.
    /// Backported from nom 6.0.0-alpha1. We can't use nom 6.0.0-alpha1 directly,
    /// because it had some issues with our use of functions in pattern.rs.
    fn re_find<'s, E>(re: Regex) -> impl Fn(&'s str) -> IResult<&'s str, &'s str, E>
    where
        E: ParseError<&'s str>,
    {
        move |i| {
            if let Some(m) = re.find(i) {
                Ok((&i[m.end()..], &i[m.start()..m.end()]))
            } else {
                Err(nom::Err::Error(E::from_error_kind(
                    i,
                    ErrorKind::RegexpMatch,
                )))
            }
        }
    }

    /// Splits the command string into separate arguments and substitutes $params
    fn parse_command<F>(command: &str, substitute: F) -> Vec<OsString>
    where
        F: Fn(&str) -> OsString,
    {
        fn join_chars(chars: Vec<char>) -> OsString {
            let mut result = OsString::new();
            for c in chars {
                result.push(c.to_string())
            }
            result
        }

        fn join_str(strings: Vec<OsString>) -> OsString {
            let mut result = OsString::new();
            for c in strings {
                result.push(c)
            }
            result
        }

        let r_var = Regex::new(r"^([[:alnum:]]|_)+").unwrap();
        let p_var = map(tuple((tag("$"), Self::re_find(r_var))), |(_, str)| {
            (substitute)(str)
        });
        let p_non_var = map(many1(none_of(" $")), join_chars);
        let p_arg = map(many1(alt((p_var, p_non_var))), join_str);
        let p_whitespace = many1(one_of(" \t"));
        let p_args = separated_list(p_whitespace, p_arg);
        let result: IResult<&str, Vec<OsString>> = (p_args)(command);
        result.expect("Parse error").1
    }
}

/// Cleans up temporary files
impl Drop for Transform {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.tmp_dir);
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::files::{file_hash, Caching, FilePos};
    use crate::util::test::with_dir;

    use super::*;

    #[test]
    fn empty() {
        assert!(Transform::new(String::from(" ")).is_err());
    }

    #[test]
    #[cfg(unix)]
    fn piped() {
        with_dir("target/test/transform/piped/", |root| {
            let p = Transform::new(String::from("dd")).unwrap();
            let input_path = root.join("input.txt");
            let mut input = File::create(&input_path).unwrap();
            let content = b"content";
            input.write(content).unwrap();
            drop(input);

            let input_path = Path::from(input_path);
            let good_file_hash = file_hash(
                &input_path,
                FilePos(0),
                FileLen::MAX,
                Caching::Default,
                |_| {},
            )
            .unwrap();

            let result = p.run(&input_path).unwrap();
            assert_eq!(result.status.code(), Some(0));
            assert_eq!(result.output_len.0, content.len() as u64);
            assert_eq!(result.output_hash, good_file_hash);
        })
    }

    #[test]
    #[cfg(unix)]
    fn parameterized() {
        with_dir("target/test/transform/param/", |root| {
            let p = Transform::new(String::from("dd if=$IN of=$OUT")).unwrap();
            let input_path = root.join("input.txt");
            let mut input = File::create(&input_path).unwrap();
            let content = b"content";
            input.write(content).unwrap();
            drop(input);

            let input_path = Path::from(input_path);
            let result = p.run(&input_path).unwrap();
            let good_file_hash = file_hash(
                &input_path,
                FilePos(0),
                FileLen::MAX,
                Caching::Default,
                |_| {},
            )
            .unwrap();

            assert_eq!(result.status.code(), Some(0));
            assert_eq!(result.output_len.0, content.len() as u64);
            assert_eq!(result.output_hash, good_file_hash);
        })
    }

    #[test]
    fn parse_command() {
        let result = Transform::parse_command("foo bar", |s| OsString::from(s));
        assert_eq!(result, vec![OsString::from("foo"), OsString::from("bar")])
    }

    #[test]
    fn parse_command_substitute() {
        let result = Transform::parse_command("foo bar in=$IN", |s| match s {
            "IN" => OsString::from("/input"),
            _ => OsString::from(s),
        });

        assert_eq!(
            result,
            vec![
                OsString::from("foo"),
                OsString::from("bar"),
                OsString::from("in=/input")
            ]
        )
    }
}
