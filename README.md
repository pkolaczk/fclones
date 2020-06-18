# FClones — Efficient Duplicate File Finder
[![CircleCI](https://circleci.com/gh/pkolaczk/fclones.svg?style=shield)](https://circleci.com/gh/pkolaczk/fclones)

Sometimes you accidentally copied your files in too many places - `fclones` will find them even if the duplicates
got different names. It can also be used to find unique or under-replicated files, e.g. to check if your 
backups contain the required files. 

`fclones` does not remove or copy files by itself. It generates a report with a list of files, and you decide
what to do with them. For your convenience, reports are available in a few different popular formats.

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
  - parallel processing in all I/O and CPU heavy stages 
  - low memory footprint thanks to heavily optimized path representation
  - fast, non-cryptographic 128-bit hashing function
  - linear complexity 
  - doesn't push data out of the page-cache (Linux-only)
  - accurate progress reporting   
* Human- and machine-readable output formats for easy further processing of results  
  - file per line 
  - `CSV`
  - `JSON`     

## Usage
Find duplicate files in the current directory, without descending into subdirectories:

    fclones * 

Find common files in two directories, without descending into subdirectories:

    fclones dir1/* dir2/*  

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
    
### Other    
    
List more options:
    
    fclones -h

### Notes
* On Unix-like systems, when using globs, one must be very careful to avoid accidental expansion of globs by the shell.
  In many cases having globs expanded by the shell instead of by `fclones` is not what you want. Therefore you
  need to quote globs:
    
      fclones . -R --names '*.jpg'       
       
* On Windows, in the default terminal, quotes are not removed before passing the arguments to the program,
  and globs are not expanded, therefore you need to pass globs unquoted:
  
      fclones . -R --names *.jpg
                    
## Installation

### Supported Platforms
The code has been thoroughly tested on Ubuntu Linux 20.04. 
 
OS            |    Architecture       |    Status                       | Limitations    
--------------|-----------------------|---------------------------------|------------
Ubuntu 20.04  |  AMD64                | fully supported                 |
Linux         |  ARMv7                | cross-compiles, not tested      | 
Linux         |  ARM64                | cross-compiles, not tested      | 
Mac OS X      |  AMD64                | works                           | no page-cache optimisation
Windows 10    |  AMD64                | works                           | no page-cache optimisation
Wine 5.0      |  AMD64                | mostly works                    | no page-cache optimisation, broken progress bar

Other systems and architectures may work. 
Help test and/or port to other platforms is welcome.
Please report successes as well as failures.      

### Download
Installation packages and binaries for some platforms 
are attached directly to [Releases](https://github.com/pkolaczk/fclones/releases).

### Building from Source 
1. [Install Rust](https://www.rust-lang.org/tools/install)
2. Run `cargo build --release`

The build will write the binary to `./target/release/fclones`. 
    
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
    
## Benchmarks

Benchmarks were performed by searching for duplicates among 1,583,334 paths containing 317 GB of 
data in my home folder. Each program was tested twice in a row. Page cache was evicted before the first run
with `echo 3 > /proc/sys/vm/drop_caches`.

### Test Setup
- Model: Dell Precision 5520
- CPU: Intel(R) Xeon(R) CPU E3-1505M v6 @ 3.00GHz
- RAM: 32 GB
- Storage: NVMe SSD 512 GB 
- System: Ubuntu Linux 20.04, kernel 5.4.0-33-generic        

### Results
Wall clock time and peak memory (RSS) were obtained from `/usr/bin/time -V` command.

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

      
