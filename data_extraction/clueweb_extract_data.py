import os
import argparse
import time
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.dirname(os.path.split(os.path.abspath(__file__))[0]))
from misc.utils import id_to_data_file_path, get_offsets, extract_records, fmt_timespan

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
            decompress the data

    The offsets are stored in a fixed-length format, so finding the offset for a record simply requires
    seeking to the location (record number * offset size) in the offset file. 

    The ClueWeb data file formats are described in detail at https://lemurproject.org/clueweb22/docspecs.php.
    """

    def __init__(self, records_file: str, root: str, datatype: str, output_path: str, decompress: bool, workers: int) -> None:
        self.records_file = records_file
        self.root = root
        self.datatype = datatype
        self.decompress = decompress
        self.workers = workers

        if datatype != 'txt' and datatype != 'html':
            raise Exception('Datatype must be "txt" or "html"')

        self.output_path = os.path.join(output_path, datatype)

        if not os.path.exists(self.records_file):
            raise Exception(f'Missing file: {self.records_file}')

        self.record_ids = []
        with open(self.records_file, 'r') as rf:
            for line in rf.readlines():
                if line.startswith('clueweb22-'):
                    line = line[10:] # strip off prefix
                self.record_ids.append(line.strip())
        
        if len(self.record_ids) == 0:
            raise Exception(f'Failed to parse any record IDs from {self.records_file}')
        
    def extract_data(self, data_path, offset_path, record_ids):
        offsets = get_offsets(offset_path, record_ids)
        record_data = extract_records(data_path, offsets)

        # write output files to <output_path>/<subdir>-<file seq>, 
        # e.g. output_path/en0000-00/
        output_path = os.path.join(self.output_path, os.path.split(data_path)[1][:-8])
        os.makedirs(output_path, exist_ok=True)
        for i, record_id in enumerate(record_ids):
            # TODO decompress optionally
            output_file = os.path.join(output_path, record_id + '.gz')
            if not os.path.exists(output_file):
                with open(output_file, 'wb') as df:
                    df.write(record_data[i])

        return len(record_data)

    def run(self) -> None:
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
        # break down the input into smaller chunks
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            future_objs = []
            for data_path in files_to_access:
                offset_path, record_ids = files_to_access[data_path]
                future_objs.append(pool.submit(self.extract_data, data_path, offset_path, record_ids))
            
            for future in concurrent.futures.as_completed(future_objs):
                records = future.result()
                total_extracted += records
                if total_extracted - last_extracted > 100:
                    elapsed = time.time() - started_at
                    remaining = (len(self.record_ids) - total_extracted) / (total_extracted / elapsed)
                    print(f'> Extracted = {total_extracted}/{len(self.record_ids)}, elapsed = {elapsed:.1f}s, remaining={fmt_timespan(remaining)}')
                    last_extracted = total_extracted
                

        # for each data file that needs to be opened, first extract the list of
        # record offsets from its corresponding offset file for the referenced 
        # record IDs, and then open the data file itself and use the offsets to
        # extract the record data

        # for data_path in files_to_access.keys():
        #     total_extracted += len(record_data)
        #     # ...
        #     if total_extracted - last_extracted > 100:
        #         elapsed = time.time() - started_at
        #         remaining = (len(self.record_ids) - total_extracted) / (total_extracted / elapsed)
        #         print(f'> Extracted = {total_extracted}/{len(self.record_ids)}, elapsed = {elapsed:.1f}s, remaining={fmt_timespan(remaining)}')
        #         last_extracted = total_extracted

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--records_file', help='Path to a file listing the ClueWeb22-IDs to read', required=True, type=str)
    parser.add_argument('-R', '--root', help='Path to ClueWeb22 dataset', required=True, type=str)
    parser.add_argument('-t', '--datatype', help='Data format to extract, either txt or html', required=True, type=str)
    parser.add_argument('-o', '--output_path', help='Path to store the output files', required=True, type=str)
    parser.add_argument('-d', '--decompress', help='Decompress the gzipped data after extracting', action='store_true')
    parser.add_argument('-w', '--workers', help='Size of worker thread pool', required=True, type=int)
    args = parser.parse_args()
    ClueWebDataExtractor(args.records_file, args.root, args.datatype, args.output_path, args.decompress, args.workers).run()
