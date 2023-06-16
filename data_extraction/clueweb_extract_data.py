import os
import gzip
import argparse
from typing import TextIO, Tuple

# each offset value in the .offset files consists of a 10 digit
# character string plus a newline
OFFSET_SIZE_BYTES = 11

class ClueWebDataExtractor:
    """
    Extract one or more records in a ClueWeb22 data file given record IDs.

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

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        if not args.file.endswith('json.gz') and not args.file.endswith('warc.gz'):
            raise Exception('Filename should end with .json.gz or .warc.gz')

        # trim the extension and add .offset to get the path to the offset file
        self.offset_file = args.file[:-7] + 'offset'
        if not os.path.exists(self.offset_file):
            raise Exception(f'Missing offset file: {self.offset_file}')
        self.record_ids = []

        if not os.path.exists(self.args.records_file):
            raise Exception(f'Missing file: {self.args.records_file}')

        with open(self.args.records_file, 'r') as rf:
            for line in rf.readlines():
                # each line should be a full ClueWeb22-ID, but we only
                # want the last 5 digits when looking up offsets
                id = int(line.strip()[-5:])
                self.record_ids.append(id)
        
        if len(self.record_ids) == 0:
            raise Exception(f'Failed to parse any record IDs from {self.args.records_file}')

        # is this JSON or WARC data?
        self.is_txt = self.args.file.endswith('json.gz')
        
    def get_offset(self, offset_fileobj: TextIO, record_id: int) -> Tuple[int, int]:
        """
        Read start+end offsets for a record from an offset file.

        Given an already-opened offset file object and a record ID, seek
        to the location in the file where the offset values for that record are
        located. Then read the next 2 values (starting and ending offsets), convert
        to ints, and return them (values are stored as 10-digit character strings).

        Args:
            offset_fileobj (TextIO): an already-opened file object for the offset file
            record_id (int): the ID (0-99999) of the record 

        Returns:
            tuple(int, int): the starting and ending offsets of the record in the data file
        """
        # seek to the location in the offset file where we'll find the offsets
        # for this record ID
        offset_fileobj.seek(record_id * OFFSET_SIZE_BYTES)
        # read the next pair of offset values (start + end)
        offset_data = offset_fileobj.read(OFFSET_SIZE_BYTES * 2)
        if len(offset_data) < OFFSET_SIZE_BYTES * 2:
            raise Exception(f'Failed to read offset data: got {len(offset_data)} bytes, expected {OFFSET_SIZE_BYTES * 2} bytes')

        # parse and return both values as integers
        offset_start, offset_end = offset_data[:OFFSET_SIZE_BYTES-1], offset_data[OFFSET_SIZE_BYTES+1:-1]
        return int(offset_start), int(offset_end)

    def extract_record(self, data_file_path: str, offset_start: int, offset_end: int) -> str:
        """
        Extract and decompress a single record from a gzipped data file given its byte offsets.

        Args:
            data_file_path (str): path to the data file to extract a record from
            offset_start (int): byte offset to the start of the target record
            offset_end (int): byte offset to the end of the target record

        Returns:
            str: decompressed record data
        """
        with open(data_file_path, 'rb') as fp:
            fp.seek(offset_start, 0)
            gzdata = fp.read(offset_end - offset_start)

        if self.is_txt:
            jsondata = gzip.decompress(gzdata).decode('utf-8')
            return jsondata

        # TODO
        return gzip.decompress(gzdata).decode('utf-8')

    def run(self) -> None:
        with open(self.args.output_file, 'w') as output_fp:
            with open(self.offset_file, 'r') as offset_fp:
                for record_id in self.record_ids:
                    offset_start, offset_end = self.get_offset(offset_fp, record_id)
                    record_data = self.extract_record(self.args.file, offset_start, offset_end)

                    if self.is_txt:
                        # newline already included
                        output_fp.write(record_data)
                    else:
                        # TODO
                        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', help='Path to the data file to read record(s) from', required=True, type=str)
    parser.add_argument('-r', '--records_file', help='Path to a file listing the ClueWeb22-IDs to read', required=True, type=str)
    parser.add_argument('-o', '--output_file', help='Path to the output file (JSONL for txt inputs)', required=True, type=str)
    args = parser.parse_args()
    ClueWebDataExtractor(args).run()
