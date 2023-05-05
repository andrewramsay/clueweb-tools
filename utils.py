import time
import subprocess
import os

def fmt_timespan(t_secs: float) -> str:
    """
    Simple timespan formatting method. 

    Args:
        t_secs (float): a timespan in seconds

    Returns:
        a string representation of the timespan
    """
    ts = ""
    if t_secs < 0:
        ts = "0"
    elif t_secs < 60:
        ts = f'{t_secs:.0f} secs'
    elif t_secs < 3600:
        ts = f'{t_secs / 60:.1f} mins'
    else:
        ts = f'{t_secs / 3600:.1f} hours'

    return ts

def sort_csv_parallel_sh(src: str, dst: str, cores: int = 8, buffer_gb: int = 10, ignore_locale: bool = True) -> int:
    """
    Use the GNU coreutils "sort" tool to sort large CSV files.

    This method provides one route to sorting very large CSV files like
    those produced by running scans of ClueWeb data files. It relies on the
    coreutils sort tool (https://man7.org/linux/man-pages/man1/sort.1.html)
    to do the bulk of the work. This tool supports parallelizing the sorting
    process, and you can also tell it to use a large memory buffer.

    The default parameters will call it with 8 parallel workers and a 10GB buffer.

    The method takes a path to a source and destination directory and optional 'sort'
    parameters, then generates a file with a .csv.sorted extension in the output directory
    for each input file (e.g. input file 'foo.csv' produces a 'foo.csv.sorted' output file).

    Existing output files which have filesizes matching their input file will not be 
    overwritten. 

    Args:
        src (str): path to the directory containing the unsorted CSV files
        dst (str): path to the directory to store the sorted CSV files
        cores (int): number of workers sort should use
        buffer_gb (int): sort memory buffer size in gigabytes
        ignore_locale (bool): sets LC_ALL=C to ignore locale settings (see comment below)

    Returns:
        number of files successfully sorted
    """

    # ignoring the system locale setting can apparently make a significant difference
    # to sorting speed. however this can also influence sorting behaviour as bytes 
    # sequences will be treated differently by different locales. in this particular
    # instance the sorting was only being done on ClueWeb22-ID fields which in turn
    # only contain ASCII characters, so there are no Unicode characters to deal with
    if ignore_locale:
        os.environ["LC_ALL"] = "C"

    os.makedirs(dst, exist_ok=True)

    successful = 0
    for d in os.listdir(src):
        for csv in os.listdir(os.path.join(src, d)):
            if not csv.endswith('csv'):
                continue

            # output filename has .sorted appended
            src = os.path.join(src, d, csv)
            dest = os.path.join(dst, csv + '.sorted')

            src_sz = os.stat(src).st_size

            # avoid redoing any already-sorted files
            if os.path.exists(dest) and os.stat(dest).st_size == src_sz:
                print(f'Skipping existing file {dest}, sizes match')
                successful += 1
                continue
            elif os.path.exists(dest):
                print('*** Incomplete existing file found, will overwrite')

            t = time.time()
            print(src, dest)
            
            # parameters for sort:
            #   -t,        = use comma as a field separator
            #   -k1,1      = search on 1st field only (i.e. Clueweb22-ID)
            #   --parallel = number of parallel workers
            #   -S         = memory buffer size
            #   -o         = output file
            subprocess.run(['sort', '-t', ',', '-k', '1,1', f'--parallel={cores}', '-S', f'{buffer_gb}G', '-o', dest, src])

            # output should be the same size since we're not editing any content
            src_sz = os.stat(src).st_size
            dest_sz = os.stat(dest).st_size
            if src_sz == dest_sz:
                successful += 1
            print(f'\t(took {time.time() - t:.2f} secs, {src_sz/(1024*1024):.1f}MB)')

    return successful


