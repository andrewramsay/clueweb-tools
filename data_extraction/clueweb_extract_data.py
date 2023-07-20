import argparse
import concurrent.futures
import gzip
import os
import sys
import time
from bz2 import BZ2Compressor
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Tuple

sys.path.append(os.path.dirname(os.path.split(os.path.abspath(__file__))[0]))
from misc.utils import extract_records, fmt_timespan, get_offsets, id_to_data_file_path

# each offset value in the .offset files consists of a 10 digit
# character string plus a newline
OFFSET_SIZE_BYTES = 11

class ClueWebDataExtractor:
    """
    Extract ClueWeb22 records of a given type using a list of ClueWeb22-IDs.

    Each gzip-compressed data file (.json.gz, .warc.gz) has a corresponding .offset file which
    gives byte offsets for each record in the data file. These can be used to randomly access
    individual records without decompressing the whole file given the ClueWeb22-IDs for the records:
        
        for each ID
            extract the record number (the final 5 digits)
            open the offset file and parse the start/end offsets for the record
            open the data file, seek to the start offset and read (end-start) bytes
            (maybe?) decompress the data

    The offsets are stored in a fixed-length format, so finding the offset for a record simply requires
    seeking to the location (record number * offset size) in the offset file. 

    The ClueWeb data file formats are described in detail at https://lemurproject.org/clueweb22/docspecs.php.
    """

    def __init__(self, records_file: str, root: str, datatype: str, output_path: str, compress_bzip2: bool, workers: int) -> None:
        self.records_file = records_file
        self.root = root
        self.datatype = datatype
        self.compress_bzip2 = compress_bzip2
        self.workers = workers

        if datatype != 'txt' and datatype != 'html':
            raise Exception('Datatype must be "txt" or "html"')

        self.output_path = os.path.join(output_path, datatype)

        if not os.path.exists(self.records_file):
            raise Exception(f'Missing file: {self.records_file}')

        self.record_ids = []
        # read the list of record IDs from the indicated file
        with open(self.records_file, 'r') as rf:
            for line in rf.readlines():
                self.record_ids.append(line.strip())
        
        if len(self.record_ids) == 0:
            raise Exception(f'Failed to parse any record IDs from {self.records_file}')
        
    def extract_data(self, data_path: str, offset_path: str, record_ids: List[str]) -> Tuple[int, int]:
        """
        Extract a set of ClueWeb-22 records from a single file.

        Args:
            data_path (str): path to the compressed data file (.json.gz or .warc.gz)
            offset_path (str): path to the corresponding .offset file
            record_ids (List[str]): the set of record IDs to extract (5 digit strings)

        Returns:
            tuple(number of extracted records, number of bytes written)
        """
        offsets = get_offsets(offset_path, record_ids)
        record_data = extract_records(data_path, offsets)

        # write output files to <output_path> with a similar directory structure
        # to the original dataset (<lang code>/<stream ID>/<subdir>/*). To do this
        # we want to copy some of the path components from the original data file path
        [lang_code, stream_id, subdir, filename] = Path(data_path).parts[-4:]

        # create the directory structure described above
        output_path = os.path.join(self.output_path, lang_code, stream_id, subdir)
        os.makedirs(output_path, exist_ok=True)

        # name the output file to match the original, e.g. for a set of records
        # take from ../en0000-00.json.gz, the output file will also be named
        # en0000-00.json.gz (except when recompressing to bz2, in which case
        # it will be given a .json.bz2 extension)
        bz2_c = BZ2Compressor()
        output_file = os.path.join(output_path, filename)
        if self.compress_bzip2:
            output_file = output_file.replace('.json.gz', '.json.bz2')

        bytes_written = 0
        with open(output_file, 'wb') as df:
            for i, _ in enumerate(record_ids):
                if self.compress_bzip2:
                    # if bzip2 compression is required, each record needs to be
                    # gunzipped and recompressed here
                    uncompressed_data = gzip.decompress(record_data[i])
                    df.write(bz2_c.compress(uncompressed_data))
                else:
                    df.write(record_data[i])

            # the BZ2Compressor object must be flushed to write out the
            # final set of bytes into the archive
            if self.compress_bzip2:
                df.write(bz2_c.flush())
            bytes_written = df.tell()

        return len(record_data), bytes_written

    def run(self) -> None:
        """
        Extract the records matching the supplied set of record IDs.

        Converts the original list of record IDs to extract into a dict which is keyed
        by data filenames, so that we only need to to open any data file once to 
        extract all the referenced records in it. Then use a ThreadPool to distribute
        the set of read operations over the configured number of workers. 
        """
        os.makedirs(self.output_path, exist_ok=True)

        # start by gathering a list of paths for the record IDs we've been given, and
        # keep track of files where multiple records are referenced so we only have to
        # open them once to extract all the necessary records
        files_to_access = {}
        for record_id in self.record_ids:
            path, offset_path = id_to_data_file_path(self.root, record_id, self.datatype)
            if path not in files_to_access:
                files_to_access[path] = (offset_path, [record_id])
            else:
                files_to_access[path][1].append(record_id)

        print(f'> {len(self.record_ids)} IDs to extract from {len(files_to_access)} files')

        started_at = time.time()
        total_extracted = 0
        last_extracted = 0
        bytes_written = 0

        # create a ThreadPool with the specified number of worker threads, and distribute
        # the set of records across the pool
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            future_objs = []
            for data_path in files_to_access:
                offset_path, record_ids = files_to_access[data_path]
                future_objs.append(pool.submit(self.extract_data, data_path, offset_path, record_ids))
            
            for future in concurrent.futures.as_completed(future_objs):
                records, new_bytes_written = future.result()
                total_extracted += records
                bytes_written += new_bytes_written
                if total_extracted - last_extracted > 100:
                    elapsed = time.time() - started_at
                    remaining = (len(self.record_ids) - total_extracted) / (total_extracted / elapsed)
                    percent = (total_extracted / len(self.record_ids)) * 100
                    print(f'> Extracted {total_extracted:,}/{len(self.record_ids):,}, {percent:.2f}%, {bytes_written/1024**3:.2f}GB written, elapsed = {fmt_timespan(elapsed)}, remaining={fmt_timespan(remaining)}')
                    last_extracted = total_extracted

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--records_file', help='Path to a file listing the ClueWeb22-IDs to read', required=True, type=str)
    parser.add_argument('-R', '--root', help='Path to ClueWeb22 dataset', required=True, type=str)
    parser.add_argument('-t', '--datatype', help='Data format to extract, either txt or html', required=True, type=str)
    parser.add_argument('-o', '--output_path', help='Path to store the output files', required=True, type=str)
    parser.add_argument('-b', '--bzip2', help='Decompress the gzipped records and recompress using bzip2', action='store_true')
    parser.add_argument('-w', '--workers', help='Size of worker thread pool', required=True, type=int)
    args = parser.parse_args()
    ClueWebDataExtractor(args.records_file, args.root, args.datatype, args.output_path, args.bzip2, args.workers).run()
