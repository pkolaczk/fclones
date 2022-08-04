use std::cell::RefCell;
use std::ffi::OsString;
use std::fs::{create_dir_all, remove_dir_all, File, OpenOptions};
use std::io;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{none_of, one_of};
use nom::combinator::map;
use nom::error::{ErrorKind, ParseError};
use nom::multi::{many1, separated_list0};
use nom::sequence::tuple;
use nom::IResult;
use regex::Regex;
use uuid::Uuid;

use crate::path::Path;

/// Controls how we pass data to the child process.
/// By default, the file to process is sent to the standard input of the child process.
/// Some programs do not accept reading input from the stdin, but prefer to be pointed
/// to a file by a command-line option - in this case `Named` variant is used.
enum Input {
    /// Pipe the input file from the given path to the stdin of the child
    StdIn(PathBuf),
    /// Pass the original path to the file as $IN param
    Named(PathBuf),
    /// Copy the original file to a temporary location and pass it as $IN param
    Copied(PathBuf, PathBuf),
}

impl Input {
    fn input_path(&self) -> &PathBuf {
        match self {
            Input::StdIn(path) => path,
            Input::Named(path) => path,
            Input::Copied(_src, target) => target,
        }
    }

    fn prepare_input_file(&self) -> io::Result<()> {
        match self {
            Input::StdIn(_path) => Ok(()),
            Input::Named(_path) => Ok(()),
            Input::Copied(src, target) => {
                std::fs::copy(src, target)?;
                Ok(())
            }
        }
    }
}

impl Drop for Input {
    /// Removes the temporary file if it was created
    fn drop(&mut self) {
        let _ = match self {
            Input::StdIn(_) => Ok(()),
            Input::Named(_) => Ok(()),
            Input::Copied(_, target) => std::fs::remove_file(target),
        };
    }
}

/// Controls how we read data out from the child process.
/// By default we read output directly from the standard output of the child process.
/// If the preprocessor program can't output data to its stdout, but supports only writing
/// to files, it can be configured to write to a named pipe, and we read from that named pipe.
enum Output {
    /// Pipe data directly to StdOut
    StdOut,
    /// Send data through a named pipe
    Named(PathBuf),
    /// Read data from the same file as the input
    InPlace(PathBuf),
}

impl Output {
    /// Returns the path to the named pipe if the process is configured to write to a pipe.
    /// Returns None if the process is configured to write to stdout or to modify the input file.
    pub fn pipe_path(&self) -> Option<PathBuf> {
        match &self {
            Output::Named(output) => Some(output.clone()),
            _ => None,
        }
    }
}

impl Drop for Output {
    /// Removes the output file if it was created
    fn drop(&mut self) {
        let _ = match self {
            Output::StdOut => Ok(()),
            Output::Named(target) => std::fs::remove_file(target),
            Output::InPlace(target) => std::fs::remove_file(target),
        };
    }
}

/// Transforms files through an external program.
/// The `command_str` field contains a path to a program and its space separated arguments.
/// The command takes a file given in the `$IN` variable and produces an `$OUT` file.
#[derive(Clone)]
pub struct Transform {
    /// a path to a program and its space separated arguments
    pub command_str: String,
    /// temporary directory for storing files and named pipes
    pub tmp_dir: PathBuf,
    /// copy the file into temporary directory before running the transform on it
    pub copy: bool,
    /// read output from the same location as the original
    pub in_place: bool,
    /// will be set to the name of the program, extracted from the command_str
    pub program: String,
}

impl Transform {
    pub fn new(command_str: String, in_place: bool) -> io::Result<Transform> {
        let has_in = RefCell::new(false);
        let has_out = RefCell::new(false);

        let parsed = parse_command(&command_str, |s: &str| {
            match s {
                "OUT" if cfg!(windows) => *has_out.borrow_mut() = true,
                "IN" => *has_in.borrow_mut() = true,
                _ => {}
            };
            OsString::from(s)
        });

        let has_in = has_in.into_inner();
        let has_out = has_out.into_inner();

        if cfg!(windows) && has_out {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "$OUT not supported on Windows yet",
            ));
        }
        if in_place && has_out {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "$OUT conflicts with --in-place",
            ));
        }
        if in_place && !has_in {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "$IN required with --in-place",
            ));
        }

        let program = parsed
            .first()
            .and_then(|p| PathBuf::from(p).file_name().map(|s| s.to_os_string()));
        let program = match program {
            Some(p) => p.into_string().unwrap(),
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
                    format!("Cannot launch {program}: {e}"),
                ))
            }
        }

        Ok(Transform {
            command_str,
            program,
            tmp_dir: Transform::create_temp_dir()?,
            copy: has_in,
            in_place,
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

    /// Creates a new unique random file name in the temporary directory
    fn random_tmp_file_name(&self) -> PathBuf {
        self.tmp_dir
            .join(format!("{:032x}", Uuid::new_v4().as_u128()))
    }

    /// Returns the output file path for the given input file path
    pub fn output(&self, input: &Path) -> PathBuf {
        self.tmp_dir.join(format!("{:x}", input.hash128()))
    }

    /// Processes the input file and returns its output and err as stream
    pub fn run(&self, input: &Path) -> io::Result<Execution> {
        let (args, input_conf, output_conf) = self.make_args(input);
        let mut command = build_command(&args, &input_conf, &output_conf)?;
        let result = execute(&mut command, input_conf, output_conf)?;
        Ok(result)
    }

    /// Creates arguments, input and output configuration for processing given input path.
    /// The first element of the argument vector contains the program name.
    fn make_args(&self, input: &Path) -> (Vec<OsString>, Input, Output) {
        let input_conf = RefCell::<Input>::new(Input::StdIn(input.to_path_buf()));
        let output_conf = RefCell::<Output>::new(Output::StdOut);

        let args = parse_command(self.command_str.as_str(), |arg| match arg {
            "IN" if self.copy => {
                let tmp_target = self.random_tmp_file_name();
                input_conf.replace(Input::Copied(input.to_path_buf(), tmp_target.clone()));
                tmp_target.into_os_string()
            }
            "IN" => {
                let input = input.to_path_buf();
                input_conf.replace(Input::Named(input.clone()));
                input.into_os_string()
            }
            "OUT" => {
                let output = self.output(input);
                output_conf.replace(Output::Named(output.clone()));
                output.into_os_string()
            }
            _ => OsString::from(arg),
        });

        let input_conf = input_conf.into_inner();
        let mut output_conf = output_conf.into_inner();

        if self.in_place {
            output_conf = Output::InPlace(input_conf.input_path().clone())
        }

        (args, input_conf, output_conf)
    }
}

/// Cleans up temporary files
impl Drop for Transform {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.tmp_dir);
    }
}

/// Keeps the results of the transform program execution
pub struct Execution {
    pub(crate) child: Arc<Mutex<Child>>,
    pub(crate) out_stream: Box<dyn Read>,
    pub(crate) err_stream: Option<JoinHandle<String>>,
    _input: Input,   // holds the temporary input file(s) until execution is done
    _output: Output, // holds the temporary output file(s) until execution is done
}

impl Drop for Execution {
    fn drop(&mut self) {
        let mut buf = [0; 4096];
        while let Ok(1..) = self.out_stream.read(&mut buf) {}
        let _ = self.child.lock().unwrap().wait();
    }
}

/// Builds the `Command` struct from the parsed arguments
fn build_command(
    args: &[OsString],
    input_conf: &Input,
    output_conf: &Output,
) -> io::Result<Command> {
    let mut args = args.iter();
    let mut command = Command::new(args.next().unwrap());
    command.args(args);
    command.stderr(Stdio::piped());

    input_conf.prepare_input_file()?;
    if let Input::StdIn(_) = input_conf {
        command.stdin(File::open(input_conf.input_path())?);
    } else {
        command.stdin(Stdio::null());
    }

    if let Output::Named(output) = output_conf {
        command.stdout(Stdio::null());
        create_named_pipe(output)?;
    } else {
        command.stdout(Stdio::piped());
    }

    Ok(command)
}

#[cfg(unix)]
fn create_named_pipe(path: &std::path::Path) -> io::Result<()> {
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

/// Spawns the command process, and returns its output as a stream.
/// The standard error is captured by a background thread and read to a string.
fn execute(command: &mut Command, input: Input, output: Output) -> io::Result<Execution> {
    let child = Arc::new(Mutex::new(command.spawn()?));

    // We call 'take' to avoid borrowing `child` for longer than a single line.
    // We can't reference stdout/stderr directly, because a mutable borrow of a field
    // creates a mutable borrow of the containing struct, but later we need to mutably
    // borrow `child` again to wait on it.
    let child_out = child.lock().unwrap().stdout.take();
    let child_err = child.lock().unwrap().stderr.take();

    let output_pipe = output.pipe_path();
    let child_ref = child.clone();

    // Capture the stderr in background in order to avoid a deadlock when the child process
    // would block on writing to stdout, and this process would block on reading stderr
    // (or the other way round).
    // The other solution could be to use non-blocking I/O, but threads look simpler.
    let stderr_reaper = std::thread::spawn(move || {
        let mut str = String::new();
        if let Some(mut stream) = child_err {
            let _ = stream.read_to_string(&mut str);
        }
        // If the child is supposed to communicate its output through a named pipe,
        // ensure the pipe gets closed and the reader at the other end receives an EOF.
        // It is possible that due to a misconfiguration
        // (e.g. wrong arguments given by the user) the child would never open the output file
        // and the reader at the other end would block forever.
        if let Some(output_pipe) = output_pipe {
            // If those fail, we have no way to report the failure.
            // However if waiting fails here, the child process likely doesn't run, so that's not
            // a problem.
            let _ignore = child_ref.lock().unwrap().wait();
            let _ignore = OpenOptions::new().write(true).open(output_pipe);
        }
        str
    });

    let child_out: Box<dyn Read> = match &output {
        Output::StdOut => Box::new(child_out.unwrap()),
        Output::Named(output) => Box::new(File::open(output)?),
        Output::InPlace(output) => {
            child.lock().unwrap().wait()?;
            Box::new(File::open(output)?)
        }
    };

    Ok(Execution {
        child,
        out_stream: child_out,
        err_stream: Some(stderr_reaper),
        _input: input,
        _output: output,
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
    let p_var = map(tuple((tag("$"), re_find(r_var))), |(_, str)| {
        (substitute)(str)
    });
    let p_non_var = map(many1(none_of(" $")), join_chars);
    let p_arg = map(many1(alt((p_var, p_non_var))), join_str);
    let p_whitespace = many1(one_of(" \t"));
    let p_args = |s| separated_list0(p_whitespace, p_arg)(s);
    let result: IResult<&str, Vec<OsString>> = (p_args)(command);
    result.expect("Parse error").1
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::file::{FileChunk, FileLen, FilePos};
    use crate::hasher::{FileHasher, HashFn};
    use crate::log::StdLog;
    use crate::util::test::with_dir;

    use super::*;

    #[test]
    fn empty() {
        assert!(Transform::new(String::from(" "), false).is_err());
    }

    #[test]
    #[cfg(unix)]
    fn piped() {
        with_dir("target/test/transform/piped/", |root| {
            let transform = Transform::new(String::from("dd"), false).unwrap();
            let input_path = root.join("input.txt");
            let mut input = File::create(&input_path).unwrap();
            let content = b"content";
            input.write_all(content).unwrap();
            drop(input);

            let log = StdLog::default();
            let hasher = FileHasher::new(HashFn::default(), Some(transform), &log);
            let input_path = Path::from(input_path);
            let chunk = FileChunk::new(&input_path, FilePos(0), FileLen::MAX);
            let good_file_hash = hasher.hash_file(&chunk, |_| {}).unwrap();
            let result = hasher.hash_transformed(&chunk, |_| {}).unwrap();
            assert_eq!(result.0, FileLen(content.len() as u64));
            assert_eq!(result.1, good_file_hash);
        })
    }

    #[test]
    #[cfg(unix)]
    fn parameterized() {
        with_dir("target/test/transform/param/", |root| {
            let transform = Transform::new(String::from("dd if=$IN of=$OUT"), false).unwrap();
            let input_path = root.join("input.txt");
            let mut input = File::create(&input_path).unwrap();
            let content = b"content";
            input.write_all(content).unwrap();
            drop(input);

            let log = StdLog::default();
            let hasher = FileHasher::new(HashFn::default(), Some(transform), &log);
            let input_path = Path::from(input_path);

            let chunk = FileChunk::new(&input_path, FilePos(0), FileLen::MAX);
            let good_file_hash = hasher.hash_file(&chunk, |_| {}).unwrap();
            let result = hasher.hash_transformed(&chunk, |_| {}).unwrap();
            assert_eq!(result.0, FileLen(content.len() as u64));
            assert_eq!(result.1, good_file_hash);
        })
    }

    #[test]
    fn parse_command() {
        let result = super::parse_command("foo bar", |s| OsString::from(s));
        assert_eq!(result, vec![OsString::from("foo"), OsString::from("bar")])
    }

    #[test]
    fn parse_command_substitute() {
        let result = super::parse_command("foo bar in=$IN", |s| match s {
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
