import time
import subprocess
import os
import gzip
from typing import Tuple, List

# each offset value in the .offset files consists of a 10 digit
# character string plus a newline
OFFSET_SIZE_BYTES = 11

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

    for root, _, files in os.walk(src):
        for csv in files:
            if not csv.endswith('csv'):
                continue

            # output filename has .sorted appended
            src = os.path.join(src, root, csv)
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

def id_to_path_components(id: str) -> Tuple[str, str, str, str]:
    """
    Convert a ClueWeb22-ID into its path components. 

    The document IDs are defined as clueweb22-<subdir>-<file seq>-<doc seq>.
    This method takes an ID and returns a tuple containing the following values:
        - language code (e.g. "en")
        - stream ID: (e.g. "en00")
        - subdirectory: (e.g. "en0003")
        - filename (e.g. "en0003-18", no extension)
    
    Args:
        id (str): a ClueWeb22-ID in the standard format
   
    Returns:
        tuple(str, str, str, str): a 4-tuple containing (language code, stream ID, subdir, base filename)
    """

    # https://lemurproject.org/clueweb22/docspecs.php#Organization
    # https://lemurproject.org/clueweb22/docspecs.php#DocIds
    # 
    # ID format: clueweb22-<subdir>-<file sequence>-<record id>
    # e.g. clueweb22-en0000-00-00000 means:
    #   - start from the filetype folder, e.g. txt
    #   - move into the folder for the language code (part of the <subdir>
    #   - move into the folder named <language code><first 2 digits of subdir>, e.g. en00
    #   - move into the folder named <subdir>
    #   - select the file named <subdir>-<file sequence>.<format> (e.g. json.gz)

    # don't care about the prefix
    if id.startswith('clueweb22-'):
        id = id[10:]
    # can also ignore the record number given in the final field of the ID
    subdir, file_seq, _ = id.split('-')

    def find_first_digit(s):
        for i, c in enumerate(s):
            if c.isdigit():
                return i
        return -1

    # language codes may be different lengths, so look for the
    # first digit to extract these
    digit_index = find_first_digit(subdir)
    lang = subdir[:digit_index]

    # the stream IDs are always 2 digits immediately after the language code
    stream_id = subdir[:digit_index+2]

    return (lang, stream_id, subdir, f'{subdir}-{file_seq}') 
    
def id_to_data_file_path(root: str, id: str, filetype:str = 'txt') -> Tuple[str, str]:
    """
    Convert a ClueWeb22-ID into a path to the data file containing that record.

    Args:
        root (str): root folder of the ClueWeb22 collection
        id (str): ClueWeb22-ID

    Returns:
        tuple(str, str): a 2-tuple containing the path to the file and its offset file
    """

    lang_code, stream_id, subdir, filename = id_to_path_components(id)

    format = 'json.gz' if filetype == 'txt' else 'warc.gz'
    path = os.path.join(root, filetype, lang_code, stream_id, subdir, f'{filename}.{format}')
    # data and offset files for txt data are named <foo>.json.gz and <foo>.offset
    # data and offset files for html data are named <foo>.warc.gz and <foo>.warc.offset
    offset_path = path.replace(format if filetype == 'txt' else 'gz', 'offset')
    return path, offset_path

def get_offsets(offset_file: str, record_ids: List[int]) -> List[Tuple[int, int]]:
    """
    Read start+end offsets for a record from an offset file.

    Given a path to an offset file and a list of record IDs, seek
    to the locations in the file where the offset values for the records are
    located. Then read the next 2 values (starting and ending offsets), convert
    to ints, and add to the returned list (values are stored as 10-digit character strings).

    Args:
        offset_fileobj (TextIO): an already-opened file object for the offset file
        record_id (List[int]): IDs (0-99999) of the records to get offsets for

    Returns:
        List[tuple(int, int)]: the starting and ending offsets of each record 
    """

    results = []
    with open(offset_file, 'r') as ofp:
        for record_id in record_ids:
            # format is <subdir>-<file seq>-<record seq>
            # e.g. en0000-00-00000
            record_seq = int(record_id.split('-')[2])
            # seek to the location in the offset file where we'll find the offsets for this record ID
            ofp.seek(record_seq * OFFSET_SIZE_BYTES)
            # read the next pair of offset values (start + end)
            offset_data = ofp.read(OFFSET_SIZE_BYTES * 2)
            if len(offset_data) < OFFSET_SIZE_BYTES * 2:
                raise Exception(f'Failed to read offset data: got {len(offset_data)} bytes, expected {OFFSET_SIZE_BYTES * 2} bytes')

            offset_start, offset_end = offset_data[:OFFSET_SIZE_BYTES-1], offset_data[OFFSET_SIZE_BYTES+1:-1]
            results.append((int(offset_start), int(offset_end)))

    return results

def extract_records(data_file_path: str, offsets: List[Tuple[int, int]]) -> List[bytes]:
    """
    Extract and decompress a single record from a gzipped data file given its byte offsets.

    Args:
        data_file_path (str): path to the data file to extract a record from
        offsets (List[Tuple[int, int]]): list of tuples of offset (start, end) byte values

    Returns:
        List[bytes]: compressed record data for each set of offsets
    """
    results = []
    with open(data_file_path, 'rb') as fp:
        for offset_start, offset_end in offsets:
            fp.seek(offset_start, 0)
            gzdata = fp.read(offset_end - offset_start)
            results.append(gzdata)

    return results
