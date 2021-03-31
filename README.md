# FClones — Efficient Duplicate File Finder
[![CircleCI](https://circleci.com/gh/pkolaczk/fclones.svg?style=shield)](https://circleci.com/gh/pkolaczk/fclones)

Sometimes you accidentally copied your files in too many places - `fclones` will find them even if the duplicates
got different names. It can also be used to find unique or under-replicated files, e.g. to check if your 
backups contain the required files. 

`fclones` has been implemented in Rust with a strong focus on performance on modern hardware.
It easily outperforms many other popular duplicate finders by a wide margin (see [Benchmarks](#benchmarks)).

## Features
* Finding duplicate files
* Finding unique files
* Finding files with more than N replicas
* Finding files with fewer than N replicas
* Advanced file selection for reducing the amount of data to process  
  - scanning multiple directory roots
  - can work with a list of files piped directly from standard input
  - recursive/non-recursive file selection
  - recursion depth limit
  - filtering names and paths by extended UNIX globs 
  - filtering names and paths by regular expressions (PCRE)
  - filtering by min/max file size
  - proper handling of symlinks and hardlinks  
* High performance
  - parallel processing capability in all I/O and CPU heavy stages
  - automatic tuning of parallelism and access strategy based on device type (SSD vs HDD)
  - low memory footprint thanks to heavily optimized path representation
  - fast, non-cryptographic 128-bit hashing function
  - linear complexity
  - doesn't push data out of the page-cache (Linux-only)
  - accurate progress reporting   
* Variety of output formats for easy further processing of results  
  - standard text format
    - groups separated by group headers with file size and hash 
    - one path per line in a group  
  - optional `fdupes` compatibility (no headers, no indent, groups separated by blank lines)    
  - machine-readable formats: `CSV`, `JSON`     

### Limitations
Contrary to `fdupes` and some other popular duplicate removers,`fclones` currently 
does not remove duplicate files automatically. It only generates a report with a list of files, 
and you decide what to do with them. For your convenience, 
reports are available in a few different popular formats.
See [#27](https://github.com/pkolaczk/fclones/issues/27). 

## Installation

### Supported Platforms
The code has been thoroughly tested on Ubuntu Linux 20.04. 
 
OS            |    Architecture       |    Status                       | Limitations    
--------------|-----------------------|---------------------------------|------------
Linux         |  AMD64                | fully supported                 | 
Mac OS X      |  AMD64                | works                           | no page-cache optimisation
Windows 10    |  AMD64                | works                           | no page-cache optimisation
Wine 5.0      |  AMD64                | mostly works                    | no page-cache optimisation, broken progress bar

Other systems and architectures may work. 
Help test and/or port to other platforms is welcome.
Please report successes as well as failures.      

### Official Packages
Installation packages and binaries for some platforms 
are attached directly to [Releases](https://github.com/pkolaczk/fclones/releases).

### Third-party Packages
* [Linux Arch Package](https://aur.archlinux.org/packages/fclones-git/) by [@aurelg](https://github.com/aurelg)   

### Building from Source 
1. [Install Rust Toolchain](https://www.rust-lang.org/tools/install)
2. Run `cargo install --git https://github.com/pkolaczk/fclones`

The build will write the binary to `.cargo/bin/fclones`. 

## Usage
Find duplicate files in the current directory, without descending into subdirectories 
(in Unix-like shells with globbing support):

    fclones * 

Find duplicate files in the current directory, without descending into subdirectories (portable):
 
    fclones . -R --depth 1 

Find common files in two directories, without descending into subdirectories 
(in Unix-like shells with globbing support):

    fclones dir1/* dir2/*  

Find common files in two directories, without descending into subdirectories (portable):

    fclones dir1 dir2 -R --depth 1  

Find duplicate files in the current directory, including subdirectories:

    fclones . -R
    
Find unique files in the current directory and its subdirectories:
    
    fclones . -R --unique 

Find files that have more than 3 replicas:

    fclones . -R --rf-over 3
    
Find files that have less than 3 replicas:

    fclones . -R --rf-under 3

### Filtering
Find duplicate files of size at least 100 MB: 

    fclones . -R -s 100M

Find duplicate pictures in the current directory:

    fclones . -R --names '*.jpg' '*.png' 
                
Run `fclones` on files selected by `find` (note: this is likely slower than built-in filtering):

    find . -name '*.c' | fclones --stdin

Follow symbolic links, but don't escape out of the home folder:

    fclones . -R -L --paths '/home/**'
    
Exclude a part of the directory tree from the scan:

    fclones / -R --exclude '/dev/**' '/proc/**'    
    
### Preprocessing files
Use `--transform` option to safely transform files by an external command.
By default, the transformation happens on a copy of file data, to avoid accidental data loss.

Strip exif before matching duplicate jpg images:

    fclones . -R --names '*.jpg' --caseless --transform 'exiv2 -d a $IN' --in-place     
    
### Other    
    
List more options:
    
    fclones -h

### Notes on quoting and path globbing
* On Unix-like systems, when using globs, one must be very careful to avoid accidental expansion of globs by the shell.
  In many cases having globs expanded by the shell instead of by `fclones` is not what you want. In such cases, you
  need to quote the globs:
    
      fclones . -R --names '*.jpg'       
       
* On Windows, the default shell doesn't remove quotes before passing the arguments to the program, 
  therefore you need to pass globs unquoted:
  
      fclones . -R --names *.jpg
      
* On Windows, the default shell doesn't support path globbing, therefore wildcard characters such as * and ? used 
  in paths will be passed literally, and they are likely to create invalid paths. For example, the following 
  command that searches for duplicate files in the current directory in Bash, will likely fail in the default
  Windows shell:
  
      fclones *
      
  If you need path globbing, and your shell does not support it, 
  use a combination of recursive search `-R` with `--depth` limit and 
  built-in path globbing provided by `--names` or `--paths`.     
                          
## The Algorithm
Files are processed in several stages. Each stage except the last one is parallel, but 
the previous stage must complete fully before the next one is started.
1. Scan input files and filter files matching the selection criteria. Walk directories recursively if requested. 
   Follow symbolic links if requested. For files that match the selection criteria, read their size.
2. Group collected files by size by storing them in a hash-map. Remove groups smaller than the desired lower-bound 
   (default 2). 
3. In each group, remove duplicate files with the same inode id. The same file could be reached through different
   paths when hardlinks are present. This step can be optionally skipped.
4. For each remaining file, compute a 128-bit hash of a tiny block of initial data. Put files with different hashes 
   into separate groups. Prune result groups if needed. 
5. For each remaining file, compute a hash of a tiny block of data at the end of the file. 
   Put files with different hashes into separate groups. Prune small groups if needed.
6. For each remaining file, compute a hash of the whole contents of the file. Note that for small files
   we might have already computed a full contents hash in step 4, therefore these files can be safely
   omitted. Same as in steps 4 and 5, split groups and remove the ones that are too small.
7. Write report to the stdout.          
    
Note that there is no byte-by-byte comparison of files anywhere. A fast and good 128-bit 
[MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) hash function
is used and you don't need to worry about hash collisions. At 10<sup>15</sup> files, the probability of collision is
0.000000001, without taking into account the requirement for the files to also match by size.
    
## Tuning
At the moment, tuning is possible only for desired parallelism level. 
The `--threads` parameter controls the sizes of the internal thread-pool(s). 
This can be used to reduce parallelism level when you don't want `fclones` to 
impact performance of your system too much, e.g. when you need to do some other work
at the same time. We recommended reducing the parallelism level if you need
to reduce memory usage. 

When using `fclones` up to version 0.6.x to deduplicate files of sizes of at least a few MBs each  
on spinning drives (HDD), it is recommended to set `--threads 1`, because accessing big files 
from multiple threads on HDD can be much slower than single-threaded access 
(YMMV, this is heavily OS-dependent, 2x-10x performance differences have been reported).
 
Since version 0.7.0, fclones uses separate per-device thread-pools for final hashing 
and it will automatically tune the level of parallelism, memory buffer sizes and partial hashing sizes 
based on the device type. These automatic settings can be overriden with `-threads` as well.

The following options can be passed to `--threads`. The more specific options override the less specific ones.
- `main:<n>` – sets the size of the main thread-pool used for random I/O: directory tree scanning, 
   file metadata fetching and in-memory sorting/hashing.
   These operations typically benefit from high parallelism level, even on spinning drives. 
   Unset by default, which means the pool will be configured to use all available CPU cores.
- `dev:<device>:<r>,<s>` – sets the size of the thread-pool `r` used for random I/O and `s` used for 
   sequential I/O on the block device with the given name. The name of the device is OS-dependent. 
   Note this is not the same as the partition name or mount point.
- `ssd:<r>,<s>` – sets the sizes of the thread-pools used for I/O on solid-state drives. Unset by default. 
- `hdd:<r>,<s>` – sets the sizes of the thread-pools used for I/O on spinning drives. 
   Defaults to `8,1`
- `removable:<r>,<s>` –  sets the size of the thread-pools used for I/O 
   on removable devices (e.g. USB sticks). Defaults to `4,1`
- `unknown:<r>,<s>` –  sets the size of the thread-pools used for I/O on devices of unknown type.
   Sometimes the device type can't be determined e.g. if it is mounted as NAS.
   Defaults to `4,1`
- `default:<r>,<s>` – sets the pool sizes to be used by all unset options
- `<r>,<s>` - same as `default:<r>,<s>`  
- `<n>` - same as `default:<n>,<n>`

## Examples
To limit the parallelism level for the main thread pool to 1:

    fclones <paths> --threads main:1  
  
To limit the parallelism level for all I/O access for all SSD devices:

    fclones <paths> --threads ssd:1 

To set the parallelism level to the number of cores for random I/O accesss and to 
2 for sequential I/O access for `/dev/sda` block device:

    fclones <paths> --threads dev:/dev/sda:0,2 
    
Multiple `--threads` options can be given, separated by spaces:

    fclones <paths> --threads main:16 ssd:4 hdd:1,1     
    
    
## Benchmarks
Different duplicate finders were given a task to find duplicates in a large set of files.
Each program was executed twice. 
Before the first run, the system page cache was evicted with `echo 3 > /proc/sys/vm/drop_caches` and the second
run was executed immediately after the first run finished. 

### SSD Benchmark
- Model: Dell Precision 5520
- CPU: Intel(R) Xeon(R) CPU E3-1505M v6 @ 3.00GHz
- RAM: 32 GB
- Storage: local NVMe SSD 512 GB 
- System: Ubuntu Linux 20.04, kernel 5.4.0-33-generic
- Task: 1,583,334 paths, 317 GB of data       

Program                                                |  Version  | Language | Threads | Cold Cache Time | Hot Cache Time | Peak Memory
-------------------------------------------------------|-----------|----------|--------:|----------------:|---------------:|-------------:
fclones                                                |  0.1.0    | Rust     | 32      | **0:31.01**     | 0:11.94        |  203 MB
fclones                                                |  0.1.0    | Rust     | 8       |   0:56.70       | **0:11.57**    |  **150 MB**
[jdupes](https://github.com/jbruchon/jdupes)           |  1.14     | C        | 1       |   5:19.08       | 3:26.42        |  386 MB
[rdfind](https://github.com/pauldreik/rdfind)          |  1.4.1    | C++      | 1       |   5:26.80       | 3:29.34        |  534 MB
[fdupes](https://github.com/adrianlopezroche/fdupes)   |  1.6.1    | C        | 1       |   7:48.82       | 6:25.02        |  393 MB
[fdupes-java](https://github.com/cbismuth/fdupes-java) |  1.3.1    | Java     | 8       |   far too long  |                |  4.2 GB    


`fdupes-java` did not finish the test. I interrupted it after 20 minutes while
it was still computing MD5 in stage 2/3. Unfortunately `fdupes-java` doesn't display
a useful progress bar, so it is not possible to estimate how long it would take.

### HDD Benchmark 
- Model: Dell Precision M4600
- CPU: Intel(R) Core(TM) i7-2760QM CPU @ 2.40GHz
- RAM: 24 GB
- System: Mint Linux 19.3, kernel 5.4.0-70-generic
- Storage: Seagate Momentus 7200 RPM SATA drive, EXT4 filesystem  
- Task: 51370 paths, 2 GB data, 6811 (471 MB) duplicate files

Commands used:

      /usr/bin/time -v fclones -R <file set root> 
      /usr/bin/time -v jdupes -R -Q <file set root>
      /usr/bin/time -v fdupes -R <file set root>
      /usr/bin/time -v rdfind <file set root>

In this benchmark, the page cache was dropped before each run.
            
Program                                                |  Version  | Language | Threads |  Time           |  Peak Memory
-------------------------------------------------------|-----------|----------|--------:|----------------:|-------------:
fclones                                                |  0.9.1    | Rust     | 1       | **0:19.45**     |  18.1 MB
[rdfind](https://github.com/pauldreik/rdfind)          |  1.3.5    | C++      | 1       |   0:33.70       |  18.5 MB
[yadf](https://github.com/jRimbault/yadf)              |  0.14.1   | Rust     |         |   1:11.69       |  22.9 MB
[jdupes](https://github.com/jbruchon/jdupes)           |  1.9      | C        | 1       |   1:18.47       |  15.7 MB
[fdupes](https://github.com/adrianlopezroche/fdupes)   |  1.6.1    | C        | 1       |   1:33.71       |  15.9 MB