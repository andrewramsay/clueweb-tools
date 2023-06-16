# ClueWeb tools

This is a collection of Python and shell scripts used to run metadata scans of the `ClueWeb2022_L` dataset in order to generate a full `(ClueWeb22-ID, URL)` index in CSV format, plus various other potentially useful scripts.

The scripts are split into a few different categories:

 * Running metadata scans (`/metadata_scanning`)
 * Using the extracted metadata to search for domains or exact URLs (`/domain_url_lookups`)
 * Extracting HTML/text data for selected records (`/data_extraction`)

For more details on the scripts, see below. For further information about the `ClueWeb22` dataset, see [here](https://lemurproject.org/clueweb22/).

## Metadata scanning scripts

The scripts in this folder are intended to handle the process of scanning a complete instance of `ClueWeb22` (B, A, or L) and extracting the metadata found in the `txt` records. Specifically, the retrieved fields are:
 
 * ClueWeb22-ID
 * URL 
 * URL-hash
 * Language

The scanning can be easily parallelized by having workers read distinct sets of files. However the size of the A and especially L collections mean that a full scan will still take a significant amount of time and generate significant I/O load. 

To allow this load to be kept to an acceptable level on the cluster where these scripts were originally used, it was necessary to limit the number of workers and to provide some control over the number of files each worker processed. Two implementations of this are available:

 * a "static" version: `clueweb_metadata_scanner.py` and `clueweb_metadata_coordinator.py`
 * a "dynamic" version: `clueweb_metadata_scanner_dynamic.py` and `clueweb_dynamic_ctrl.py`

In the "static" version, the intent is that an instance of `clueweb_metadata_coordinator.py` is launched as a cluster job, and then multiple instances of `clueweb_metadata_scanner.py` can be started in additional jobs as required. Each of the later is given a number of files to scan and a number of cores to use, and communicates with the coordinator using ZeroMQ messaging. 

In the "dynamic" version, the intent is that only a single job is required which will run the `clueweb_metadata_scanner_dynamic.py` module. This module is initialized with a fixed maximum number of worker processes, but doesn't actually create any of them by default. You can then use the `clueweb_dynamic_ctrl.py` module to dynamically enable/disable the required number of workers. Each worker simply runs a loop which requests a new file to scan from the master process until there are no files left or the worker is paused. This allows I/O load to be adjusted easily by lowering or raising the number of active workers, which can be done by running the `clueweb_dynamic_ctrl.py` script. 

### Scanning workflow

```bash
# 1. Run the clueweb_dbwrapper.py script. This will walk the directory structure of a ClueWeb collection
# and build an SQLite database containing a list of the data files to be scanned and the number of records
# in each file. This database is then used by the scanning scripts to feed files to workers. 
#
# python clueweb_dbwrapper.py -r <path to ClueWeb dataset> -o <output database filename>
python -r /path/to/ClueWeb22_L -o clueweb_L_files.db

# (assuming the "dynamic" scripts are being used)
# 2. Launch an instance of clueweb_metadata_scanner_dynamic.py 
#
# python clueweb_metadata_scanner_dynamic.py -d <database filename> -o <path for output .csv files> -p <max number of workers> [-P <ZMQ port>]
python clueweb_metadata_scanner_dynamic.py -d clueweb_L_files.db -o scan_outputs/ -p 10

# 3. At this point the workers will all be paused. The clueweb_dynamic_ctrl.py script allows you
# to enable/disable individual workers, or you can use a simple shell command to update the state
# of multiple workers in one go.
# 
# resume/unpause a worker:
# python clueweb_dynamic_ctrl.py -a <host/IP where the scanner script is running> -R <worker ID> 
#
# pause a worker:
# python clueweb_dynamic_ctrl.py -a <host/IP where the scanner script s running> -P <worker ID>
# 
# Worker IDs are integers in the range [0, num_workers - 1]
# e.g. to enable the first worker:
python clueweb_dynamic_ctrl.py -a 127.0.0.1 -R 0
# to enable the first 5 workers
for i in {0..4}; do python clueweb_dynamic_ctrl.py -a 127.0.0.1 -R $i; done

# 4. Monitoring progress can be done using the clueweb_check_progress.py script. This will simply 
# query the SQLite database to check how many data files have been marked as scanned already and
# print out a short status message. 

# 5. After the scan has completed, the output will be a set of CSV files (one per worker), where each
# line contains the metadata fields extracted from a single record. Merging all of these into a single
# sorted file can be done using the clueweb_heap_sort.py script. Due to the size of the output files 
# when doing a scan of the full ClueWeb22 collection, in-memory sorting and merging is likely infeasible.
# This approach has minimal memory requirements and only requires that the input files are already 
# sorted, and the scanner scripts are intended to ensure that this is the case by sorting the list of
# input files (which in turn contain sorted records) and feeding them to the workers in the same order.
python clueweb_heap_sort.py -i <path to worker .csv output files> -o <output CSV filename> -t <expected total number of records>
```

## Domain and URL lookups

TODO

## HTML data extraction for selected records

TODO



