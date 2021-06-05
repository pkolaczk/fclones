# FClones — Efficient Duplicate File Finder and Remover
[![CircleCI](https://circleci.com/gh/pkolaczk/fclones.svg?style=shield)](https://circleci.com/gh/pkolaczk/fclones)
[![crates.io](https://img.shields.io/crates/v/fclones.svg)](https://crates.io/crates/fclones)
[![Documentation](https://docs.rs/fclones/badge.svg)](https://docs.rs/fclones)

Sometimes you accidentally copied your files in too many places - `fclones group` will scan the file-system 
and quickly identify groups of identical files. Depending on the settings, 
it can be used to find redundant (duplicate), unique or under-replicated files, e.g., to 
check if your backups contain the required number of copies. It also offers a rich set of filtering 
options letting you analyze only a small subset of the directory tree.

The redundant files found by `fclones group` can be then removed by executing `fclones remove` or 
replaced by soft of hard links with `fclones link`. 
For maximum safety, there is also a `--dry-run` option that prints out all the file-system changes 
to be performed without actually running them. 

FClones has been implemented in Rust with a strong focus on high performance on modern hardware. 
It employs several techniques not present in many other
programs (which often claim to be "fast" duplicate finders). FClones adapts to the type of the hard drive, 
orders file operations by physical data placement on HDDs, scans directory tree in parallel and uses prefix compression
of paths to reduce memory consumption when working with millions of files.
As a result, FClones easily outperforms many other popular duplicate finders by a wide margin on either SSD or HDD storage 
(see [Benchmarks](#benchmarks)).

## Features
* Identifying groups of identical files
  - finding duplicate files
  - finding files with more than N replicas
  - finding unique files
  - finding files with fewer than N replicas
* Advanced file selection for reducing the amount of data to process
  - scanning multiple directory roots
  - can work with a list of files piped directly from standard input
  - recursive/non-recursive file selection
  - recursion depth limit
  - filtering names and paths by extended UNIX globs
  - filtering names and paths by regular expressions
  - filtering by min/max file size
  - proper handling of symlinks and hardlinks
* Removing redundant files
  - removing or replacing files with soft or hard links
  - selecting files for removal by path or name patterns  
  - prioritizing files to remove by creation, modification, last access time or nesting level
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
Some optimisations are not available on platforms other than Linux:
  - ordering of file accesses by physical placement
  - page-cache drop-behind

## Demo
Let's first create some files:

    $ mkdir test
    $ cd test
    $ echo foo >foo1.txt
    $ echo foo >foo2.txt
    $ echo foo >foo3.txt
    $ echo bar >bar1.txt
    $ echo bar >bar2.txt

Now let's identify the duplicates:

    $ fclones group . >dupes.txt
    [2021-06-05 18:21:33.358] fclones:  info: Started grouping
    [2021-06-05 18:21:33.738] fclones:  info: Scanned 7 file entries
    [2021-06-05 18:21:33.738] fclones:  info: Found 5 (20 B) files matching selection criteria
    [2021-06-05 18:21:33.738] fclones:  info: Found 4 (16 B) candidates after grouping by size
    [2021-06-05 18:21:33.738] fclones:  info: Found 4 (16 B) candidates after grouping by paths and file identifiers
    [2021-06-05 18:21:33.739] fclones:  info: Found 3 (12 B) candidates after grouping by prefix
    [2021-06-05 18:21:33.740] fclones:  info: Found 3 (12 B) candidates after grouping by suffix
    [2021-06-05 18:21:33.741] fclones:  info: Found 3 (12 B) redundant files

    $ cat dupes.txt
    # Report by fclones 0.12.0
    # Timestamp: 2021-06-05 18:21:33.741 +0200
    # Command: fclones group .
    # Found 2 file groups
    # 12 B (12 B) in 3 redundant files can be removed
    7d6ebf613bf94dfd976d169ff6ae02c3, 4 B (4 B) * 2:
        /tmp/test/bar1.txt
        /tmp/test/bar2.txt
    6109f093b3fd5eb1060989c990d1226f, 4 B (4 B) * 3:
        /tmp/test/foo1.txt
        /tmp/test/foo2.txt
        /tmp/test/foo3.txt

Finally we can replace the duplicates by soft links:

    $ fclones link --soft <dupes.txt 
    [2021-06-05 18:25:42.488] fclones:  info: Started deduplicating
    [2021-06-05 18:25:42.493] fclones:  info: Processed 3 files and reclaimed 12 B space

    $ ls -l
    total 12
    -rw-rw-r-- 1 pkolaczk pkolaczk   4 cze  5 18:19 bar1.txt
    lrwxrwxrwx 1 pkolaczk pkolaczk  18 cze  5 18:25 bar2.txt -> /tmp/test/bar1.txt
    -rw-rw-r-- 1 pkolaczk pkolaczk 382 cze  5 18:21 dupes.txt
    -rw-rw-r-- 1 pkolaczk pkolaczk   4 cze  5 18:19 foo1.txt
    lrwxrwxrwx 1 pkolaczk pkolaczk  18 cze  5 18:25 foo2.txt -> /tmp/test/foo1.txt
    lrwxrwxrwx 1 pkolaczk pkolaczk  18 cze  5 18:25 foo3.txt -> /tmp/test/foo1.txt

## Installation
The code has been thoroughly tested on Ubuntu Linux 20.10.
Other systems like Windows or Mac OS X and other architectures may work. 
Help test and/or port to other platforms is welcome.
Please report successes as well as failures.      

### Official Packages
Installation packages and binaries for some platforms 
are attached directly to [Releases](https://github.com/pkolaczk/fclones/releases).

### Third-party Packages
* [Linux Arch Package](https://aur.archlinux.org/packages/fclones-git/) by [@aurelg](https://github.com/aurelg)   

### Building from Source 
1. [Install Rust Toolchain](https://www.rust-lang.org/tools/install)
2. Run `cargo install fclones`

The build will write the binary to `.cargo/bin/fclones`. 

## Usage

FClones offers separate commands for finding and removing files. This way, you can inspect
the list of found files before applying any modifications to the file system. 

  - `group` - identifies groups of identical files and prints them to the standard output
  - `remove` - removes redundant files earlier identified by `group`
  - `link` - replaces redundant files with links (default: hard links)

### Finding Files

Find duplicate, unique, under-replicated or over-replicated files in the current directory, 
including subdirectories:

    fclones group .
    fclones group . --unique 
    fclones group . --rf-under 3
    fclones group . --rf-over 3

You can search in multiple directories:

    fclones group dir1 dir2 dir3

Limit the recursion depth:
    
    fclones group . --depth 1   # scan only files in the current dir, skip subdirs
    fclones group * --depth 0   # similar as above in shells that expand `*` 

Caution: Versions up to 0.10 did not descend into directories by default.
In those old versions, add `-R` flag to enable recursive directory walking.
 
Finding duplicate files of size at least 100 MB: 

    fclones group . -s 100M

Filter by file name or path pattern:

    fclones group . --name '*.jpg' '*.png' 
                
Run `fclones` on files selected by `find` (note: this is likely slower than built-in filtering):

    find . -name '*.c' | fclones group --stdin --depth 0

Follow symbolic links, but don't escape out of the home folder:

    fclones group . -L --path '/home/**'
    
Exclude a part of the directory tree from the scan:

    fclones group / --exclude '/dev/**' '/proc/**'    

### Removing Files
To remove files or replace them by links, you need to send the default report produced by
`fclones group` to the standard input of `fclones remove` or `fclones link` command.

Assuming the list of duplicates has been saved in file `dupes.txt`, the following commands would remove
the redundant files: 

    fclones link <dupes.txt         # replace with hard links
    fclones link -s <dupes.txt      # replace with soft links
    fclones remove <dupes.txt       # remove totally

If you prefer to do everything at once without storing the list of groups in a file, you can pipe:

    fclones group . | fclones link

To select the number of files to preserve, use the `-n`/`--rf-over` option.
By default, it is set to the value used when running `group` (which is 1 if it wasn't set explicitly). 
To leave 2 replicas in each group, run: 

    fclones remove -n 2 <dupes.txt

By default, FClones follows the order of files specified in the input file. It keeps the files given at the beginning
of each list, and removes / replaces the files given at the end of each list. It is possible to change that 
order by `--priority` option, for example:

    fclones remove --priority newest <dupes.txt        # remove the newest replicas
    fclones remove --priority oldest <dupes.txt        # remove the oldest replicas

For more priority options, see `fclones remove --help`.

It is also possible to restrict removing files to only files with names or paths matching a pattern:

    fclones remove --name '*.jpg' <dupes.txt       # remove only jpg files
    fclones remove --path '/trash/**' <dupes.txt   # remove only files in the /trash folder

If it is easier to specify a pattern for files which you do *not* want to remove, then use one of `keep` options:

    fclones remove --keep-name '*.mov' <dupes.txt           # never remove mov files
    fclones remove --keep-path '/important/**' <dupes.txt   # never remove files in the /important folder

To make sure you're not going to remove wrong files accidentally, use `--dry-run` option.
This option prints all the commands that would be executed, but it doesn't actually execute them:

    fclones link --soft <dupes.txt --dry-run 2>/dev/null

    mv /tmp/test/bar2.txt /tmp/test/bar2.txt.jkXswbsDxhqItPeOfCXsWN4d
    ln -s /tmp/test/bar1.txt /tmp/test/bar2.txt
    rm /tmp/test/bar2.txt.jkXswbsDxhqItPeOfCXsWN4d
    mv /tmp/test/foo2.txt /tmp/test/foo2.txt.ze1hvhNjfre618TkRGUxJNzx
    ln -s /tmp/test/foo1.txt /tmp/test/foo2.txt
    rm /tmp/test/foo2.txt.ze1hvhNjfre618TkRGUxJNzx
    mv /tmp/test/foo3.txt /tmp/test/foo3.txt.ttLAWO6YckczL1LXEsHfcEau
    ln -s /tmp/test/foo1.txt /tmp/test/foo3.txt
    rm /tmp/test/foo3.txt.ttLAWO6YckczL1LXEsHfcEau

    
### Preprocessing Files
Use `--transform` option to safely transform files by an external command.
By default, the transformation happens on a copy of file data, to avoid accidental data loss.
Note that this option may significantly slow down processing of a huge number of files, 
because it invokes the external program for each file.

The following command will strip exif before matching duplicate jpg images:

    fclones group . --name '*.jpg' --caseless --transform 'exiv2 -d a $IN' --in-place     
    
### Other    
    
List more options:
    
    fclones [command] -h      # short help
    fclones [command] --help  # detailed help

### Path Globbing
FClones understands a subset of Bash Extended Globbing.
The following wildcards can be used:
- `?`         matches any character except the directory separator
- `[a-z]`     matches one of the characters or character ranges given in the square brackets
- `[!a-z]`    matches any character that is not given in the square brackets
- `*`         matches any sequence of characters except the directory separator
- `**`        matches any sequence of characters including the directory separator
- `{a,b}`     matches exactly one pattern from the comma-separated patterns given inside the curly brackets
- `@(a|b)`    same as `{a,b}`
- `?(a|b)`    matches at most one occurrence of the pattern inside the brackets
- `+(a|b)`    matches at least occurrence of the patterns given inside the brackets
- `*(a|b)`    matches any number of occurrences of the patterns given inside the brackets
- `{escape}`  escapes wildcards, e.g. `{escape}?` would match `?` literally

#### Caution

* On Unix-like systems, when using globs, one must be very careful to avoid accidental expansion of globs by the shell.
  In many cases having globs expanded by the shell instead of by `fclones` is not what you want. In such cases, you
  need to quote the globs:
    
      fclones group . --name '*.jpg'       
       
* On Windows, the default shell doesn't remove quotes before passing the arguments to the program, 
  therefore you need to pass globs unquoted:
  
      fclones group . --name *.jpg
      
* On Windows, the default shell doesn't support path globbing, therefore wildcard characters such as * and ? used 
  in paths will be passed literally, and they are likely to create invalid paths. For example, the following 
  command that searches for duplicate files in the current directory in Bash, will likely fail in the default
  Windows shell:
  
      fclones group *
      
  If you need path globbing, and your shell does not support it,
  use the builtin path globbing provided by `--name` or `--path`.     
                          
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

### Examples
To limit the parallelism level for the main thread pool to 1:

    fclones group <paths> --threads main:1  
  
To limit the parallelism level for all I/O access for all SSD devices:

    fclones group <paths> --threads ssd:1 

To set the parallelism level to the number of cores for random I/O accesss and to 
2 for sequential I/O access for `/dev/sda` block device:

    fclones group <paths> --threads dev:/dev/sda:0,2 
    
Multiple `--threads` options can be given, separated by spaces:

    fclones group <paths> --threads main:16 ssd:4 hdd:1,1     
    
    
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