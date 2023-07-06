import sys
import os
import csv
import hashlib
import sqlite3
import argparse
from typing import List, Tuple, Union

from flask import Flask, request, send_file, Response

# the first ID in A that is NOT in B
FIRST_A_ID = 'en0100-00-00000'
# the first ID in L that is NOT in A
FIRST_L_ID = 'en1000-00-00000'

class ClueWebDomainURLLookup:
    """Provides some simple methods to search for domains/URLs in ClueWeb-22.

    This class relies on having an SQLite database with the following schema:

        CREATE TABLE urls (id INTEGER PRIMARY KEY, clueweb_id TEXT UNIQUE, url TEXT);
        CREATE UNIQUE INDEX cwindex on urls (url, clueweb_id);

    This provides a simple mapping from ClueWeb22-IDs to URLs, and the index allows
    queries to run much faster than they would otherwise because it avoids having to
    do full-table scans. 

    Building the database can take a significant amount of time and disk space (870GB for
    the English-only subset of ClueWeb22-L).
    """

    def __init__(self, db_path: str) -> None:
        if not os.path.exists(db_path):
            raise Exception('Missing database')
        self.conn = sqlite3.connect(db_path, check_same_thread=False)

    def _mode_to_id(self, mode: str) -> str:
        return FIRST_A_ID if mode == 'B' else FIRST_L_ID

    def read_inputs(self, path: str, column: int) -> List[str]:
        inputs = []
        with open(path) as f:
            reader = csv.reader(f)
            for row in reader:
                inputs.append(row[column])
        return inputs

    def write_results(self, results, path):
        with open(path, 'w') as f:
            writer = csv.writer(f)
            for r in results:
                writer.writerow(r)

    def search_domain(self, domain: str, mode: str = 'L') -> List[Tuple[str, str]]:
        results = []

        if mode != 'L':
            limiting_id = self._mode_to_id(mode)
            for row in self.conn.execute('SELECT clueweb_id, url FROM urls WHERE (url >= ? AND url GLOB ?) AND clueweb_id < ?', (domain, domain + '/*', limiting_id)):
                results.append(row)
        else:
            for row in self.conn.execute('SELECT clueweb_id, url FROM urls WHERE url >= ? AND url GLOB ?', (domain, domain + '/*')):
                results.append(row)
        return results
                results.append(row)
        return results

    def count_domain(self, domain: str, mode: str = 'L') -> int:
        if mode != 'L':
            limiting_id = self._mode_to_id(mode)
            res = self.conn.execute('SELECT COUNT(clueweb_id) FROM urls WHERE (url >= ? AND url GLOB ?) AND clueweb_id < ?', (domain, domain + '/*', limiting_id))
        else:
            res = self.conn.execute('SELECT COUNT(clueweb_id) FROM urls WHERE url >= ? AND url GLOB ?', (domain, domain + '/*'))
        count = res.fetchone()[0]
        return count

    def search_url(self, url: str, mode: str = 'L') -> Union[Tuple[str, str], None]:
        if mode != 'L':
            limiting_id = self._mode_to_id(mode)
            cursor = self.conn.execute('SELECT clueweb_id, url FROM urls WHERE url = ? AND clueweb_id < ?', (url, limiting_id))
        else:
            cursor = self.conn.execute('SELECT clueweb_id, url FROM urls WHERE url = ?', (url, ))

        result = cursor.fetchone()
        if result is None:
            return None

        return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='Path to ClueWeb SQLite index database', required=True, type=str)
    parser.add_argument('-u', '--input_urls', help='Path to a CSV file listing the URLs to check', type=str)
    parser.add_argument('-D', '--input_domains', help='Path to a CSV file listing the domains to check', type=str)
    parser.add_argument('-c', '--count_domain_matches', help='Only output numbers of matching records for supplied domains', action='store_true')
    parser.add_argument('-C', '--input_column', help='Column to read from the CSV file (default 0)', default=0, type=int)
    parser.add_argument('-o', '--output_file', help='Path to write CSV file with results', required=True, type=str)
    parser.add_argument('-m', '--mode', help='Select subset of ClueWeb22: "B", "A", or "L" (default is L)', default='L', type=str)
    args = parser.parse_args()

    if args.input_urls is not None and args.input_domains is not None:
        print('You must only provide one of -u/--input_urls and -D/--input_domains')
        sys.exit(0)
    elif args.input_urls is None and args.input_domains is None:
        print('You must provide either -u/--input_urls or -D/--input_domains')
        sys.exit(0)

    if args.mode not in ['B', 'A', 'L']:
        print('Invalid -m/--mode value, must be one of "B", "A", "L"')
        sys.exit(0)

    lookup = ClueWebDomainURLLookup(args.database)
    input_list = lookup.read_inputs(args.input_urls if args.input_urls is not None else args.input_domains, args.input_column)

    if args.input_urls is not None:
        print(f'> Looking up {len(input_list)} URLs in ClueWeb22-{args.mode}')
        results = list(filter(lambda x: x is not None, [lookup.search_url(url, args.mode) for url in input_list]))
        lookup.write_results(results, args.output_file)
        print(f'> Wrote {len(results)} URL record matches ({100 * (len(results)/len(input_list)):.1f}%) to {args.output_file}')
    else:
        if args.count_domain_matches:
            print(f'> Counting matching records for {len(input_list)} domains in ClueWeb22-{args.mode}')
            results = [(domain, lookup.count_domain(domain, args.mode)) for domain in input_list]
            lookup.write_results(results, args.output_file)
            print(f'> Wrote {len(results)} domain count results to {args.output_file}')
        else:
            print(f'> Looking up {len(input_list)} domains in ClueWeb22-{args.mode}')
            results = []
            for domain in input_list:
                domain_hits = lookup.search_domain(domain, args.mode)
                results.extend(domain_hits)
                print(f'> Found {len(domain_hits)} results for {domain}')

            lookup.write_results(results, args.output_file)
            print(f'> Wrote {len(results)} total domain hits to {args.output_file}')
