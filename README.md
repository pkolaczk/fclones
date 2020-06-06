# fclones - Find Redundant or Under-replicated Files

Sometimes you accidentally copied your files in too many places - `fclones` will find them even if the duplicates
got different names. It can also be used to find unique or under-replicated files, e.g. to check if your 
backups contain the required files. 

`fclones` does not remove or copy files by itself. It only generates a report with a list of files, and you decide
what to do with them.   

## Notable Features
* Advanced filtering
    * extended globs 
    * regular expressions
    * can work with a list of files piped directly from `find`
* Output formats
    * one file per line 
    * `CSV`
    * `JSON` 
* High performance
    * linear complexity
    * parallel processing 
    * low memory footprint
    * doesn't push data out of the page-cache 

## Examples
Find duplicate files in the current directory, without descending into subdirectories:

    fclones * 

Find common files in two directories, without descending into subdirectories:

    fclones dir1/* dir2/*  

Find duplicate files in the current directory, including subdirectories:

    fclones . -R
    
Find duplicate pictures in the current directory:

    fclones . -R --names '*.jpg' '*.png' 
    
Find unique files in the current directory and its subdirectories:
    
    fclones . -R --unique 
        
Run `fclones` on files selected by `find` (note: this is likely slower than built-in filtering):

    find . -name '*.c' | fclones --stdin
    
Find files that have more than 3 replicas:

    fclones . -R --rf-over 3
    
Find files that have less than 3 replicas:

    fclones . -R --rf-under 3
    
Follow symbolic links, but don't escape out of the home folder:

    fclones . -R -L --paths '/home/**'
    
List more options:
    
    fclones -h
    
## Supported Platforms
The code has been tested only on Linux, but should be straightforward to 
compile to other operating systems. PRs are welcome.     
    
## Building 
1. [Install Rust](https://www.rust-lang.org/tools/install)
2. Run `cargo build --release`

The build will write the binary to `./target/release/fclones`. 
    
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

Program             | Command              | Cold Cache Time | Hot Cache Time | Peak Memory
--------------------|----------------------|----------------:|---------------:|-------------:
fclones 0.1.0       | `fclones -R -t 32 ~` |   **0:31.01**   | 0:11.94        |  203 MB
fclones 0.1.0       | `fclones -R ~`       |   0:56.70       | **0:11.57**    |  **150 MB**
jdupes 1.14         | `jdupes -R ~`        |   5:19.08       | 3:26.42        |  386 MB
rdfind 1.4.1        | `rdfind ~`           |   5:26.80       | 3:29.34        |  534 MB
fdupes 1.6.1        | `fdupes -R ~`        |   7:48.82       | 6:25.02        |  393 MB
fdupes-java 1.3.0  | `java -Dfdupes.parallelism=8 -jar fdupes-java-1.3.1.jar ~`  |   &gt; 20:00.00    |  | 4.2 GB |   

`fdupes-java` did not finish the test, I interrupted it after 20 minutes.

      
