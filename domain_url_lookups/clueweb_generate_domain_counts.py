import argparse
import os
import sys
import time
from multiprocessing import Pool
from urllib.parse import urlparse

sys.path.append('/mnt/archive/ClueWeb22_L/csvmonkey')
import csvmonkey


class ClueWebDomainCounter:
    
    def __init__(self, args: argparse.Namespace):
        self.args = args

    def file_scanner(self, pid: int, file: str, limit: int, overwrite: bool, num_records: int) -> None:
        num_lines = 0
        p_increment = 1_000_000
        seen = {}

        for row in csvmonkey.from_path(file, header=False, yields='tuple'):
            num_lines += 1
            if num_lines % p_increment == 0:
                if num_records == 0:
                    print(f'> Worker {pid} has scanned {num_lines} records')
                else:
                    percent = 100 * (num_lines / num_records)
                    print(f'> Worker {pid} has scanned {num_lines} records, {percent:.1f}%')

            _, url, _, lang = row
            # filter by language code
            if self.args.language == 'all' or self.args.language == lang:
                urlobj = urlparse(url)
                # expecting domains to be just "www.reddit.com" etc
                if urlobj.netloc not in seen:
                    seen[urlobj.netloc] = 1
                else:
                    seen[urlobj.netloc] += 1

            if limit > 0 and num_lines >= limit:
                break

        output_file = file.replace('.csv', '.domains')
        mode = 'w' if overwrite else 'a'
        print(f'> Writing results to {output_file}')
        with open(output_file, mode) as f:
            for d, c in seen.items():
                f.write(f'{d},{c}\n')

    def run(self):
        start_time = time.time()
        process_args = []
        for f in os.listdir(self.args.files):
            if f.endswith('.csv'):
                process_args.append((len(process_args), os.path.join(self.args.files, f), self.args.limit, self.args.overwrite, args.num_records))

        pool = Pool(processes=args.processes)
        result = pool.starmap_async(self.file_scanner, process_args)
        for val in result.get():
            pass

        print(f'\n\n> Scan completed in {time.time() - start_time:.2f} seconds')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--files', help='Path to the folder containing index .csv files', required=True, type=str)
    parser.add_argument('-p', '--processes', help='Number of parallel searchers to run', default=20, type=int)
    parser.add_argument('-O', '--overwrite', help='Overwrite existing output file instead of appending', action='store_true')
    parser.add_argument('-l', '--limit', help='limit scans to first n lines of each file', default=0, type=int)
    parser.add_argument('-L', '--language', help='language code to use (or "all")', default='en', type=str)
    parser.add_argument('-n', '--num_records', help='expected number of lines in each file (just for progress info)', default=0, type=int)
    args = parser.parse_args()
    ClueWebDomainCounter(args).run()
