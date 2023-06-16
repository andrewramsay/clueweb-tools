import os
import csv
import argparse
import sqlite3
from typing import List

import tqdm

class ClueWebUrlExtract:
    """
    This class can be used to generate a text file listing all the (id, URL) pairs 
    for a given set of domains. It relies on having built an SQLite3 database containing
    a single "urls" table with "clueweb_id" and "url" columns. Adding indexes is
    necessary to allow for the lookups to run at a reasonable speed.
    """

    def __init__(self, db_file: str, domains_file: str, output_file: str, summary_file: str) -> None:
        if not os.path.exists(db_file):
            raise Exception(f'Missing database file {db_file}')
        if not os.path.exists(domains_file):
            raise Exception(f'Missing domains file {domains_file}')
        self.conn = sqlite3.connect(db_file)

        self.domains_file = domains_file
        self.output_file = output_file
        self.summary_file = summary_file

    def extract_domain(self, domain: str, csvwriter: '_csv.writer') -> int:
        """
        Given a domain, write all matching rows to the csvwriter.

        The matching is done using the SQLite GLOB operator with a wildcard appended
        to the original domain, so "https://foo.bar.com" will match any URL
        starting with that prefix, e.g.
            https://foo.bar.com
            https://foo.bar.com/some/path
            https://foo.bar.com.io/some/path

        Args:
            domain (str): a domain with an http:// or https:// prefix
            csvwriter: CSV writer object to write results into

        Returns:
            number of matched records in the database
        """
        count = 0

        # this query relies on the URL column being indexed. this allows the ">=" clause to 
        # very quickly find the first matching row and then apply the globbing from there,
        # instead of scanning the entire database
        for row in self.conn.execute('SELECT clueweb_id, url FROM urls WHERE url >= ? AND url GLOB ?', (domain, domain + '*')):
            csvwriter.writerow(row)
            count += 1

        print(f'> Wrote {count} records for {domain}')
        return count

    def run(self):
        domains = [x.strip() for x in open(self.domains_file, 'r').readlines()]
        pb = tqdm.tqdm(total=len(domains))

        counts = []

        with open(self.output_file, 'w') as outputcsv:
            writer = csv.writer(outputcsv)
            for domain in domains:
                pb.set_description(domain)
                if not domain.startswith('http:') and not domain.startswith('https:'):
                    # assume https if no protocol given
                    count = self.extract_domain('https://' + domain, writer)
                    # TODO make optional
                    # automatically also check for www.foo.com as well as foo.com
                    if not domain.startswith('www'):
                        count += self.extract_domain('https://www.' + domain, writer)
                else:
                    count = self.extract_domain(domain, writer)
                    # TODO make optional
                    # automatically also check for www.foo.com as well as foo.com
                    if not domain.startswith('https://www'):
                        count += self.extract_domain(domain.replace('https://', 'https://www.'), writer)
                pb.update(1)

                counts.append((domain, count))

        pb.close()

        with open(self.summary_file, 'w') as f:
            writer = csv.writer(f)
            for val in counts:
                writer.writerow(val)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='Path to SQLite database of (ID, URL) pairs', required=True, type=str)
    parser.add_argument('-D', '--domains', help='Path to file listing domains to search', required=True, type=str)
    parser.add_argument('-o', '--output', help='Path for output .csv file listing ClueWeb-ID,url pairs', required=True, type=str)
    parser.add_argument('-s', '--summary_file', help='Path for output .csv file listing record counts for each domain', required=True, type=str)
    args = parser.parse_args()
    ClueWebUrlExtract(args.database, args.domains, args.output, args.summary_file).run()
