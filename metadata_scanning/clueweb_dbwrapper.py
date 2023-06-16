import argparse
import csv
import os
import sqlite3
import time
from typing import List, Tuple


class ClueWebFileDatabase:

    NOT_STARTED = 0     # data file hasn't been scanned yet
    IN_PROGRESS = 1     # data file is currently being scanned
    DONE        = 2     # data file has been scanned

    def __init__(self, db_file) -> None:
        self.conn = sqlite3.connect(db_file)

    @staticmethod
    def generate(clueweb_root: str, output_filename: str) -> None:
        """
        Generate an SQLite database containing a list of data files and their record counts.

        This method will first walk the directory structure of a ClueWeb dataset to build up
        a list of filenames relative to the root path of all the .json.gz files
        containing text format data (in the 'txt' subdirectory). 

        Once it has these it will then read the files under <root>/record_counts/txt
        to extract the expected number of records in each file. 

        Finally, a single table SQLite database is generated containing the filenames
        and record counts, plus some other columns used to manage the process of
        extracting batches of files to be scanned by cluster jobs.

        Args:
            clueweb_root (str): path to the ClueWeb dataset root directory 
            output_filename (str): filename of the new database file

        Returns:
            None (raise an Exception if the output file already exists)
        """

        if os.path.exists(output_filename):
            raise Exception(f'Refusing to overwrite output file: {output_filename}')

        start_time = time.time()

        print(f'> Looking for ClueWeb txt data files under {os.path.join(clueweb_root, "txt")}')

        # build a list of all the data files (those with .json.gz extensions), reporting on
        # progress every 10k files
        last_length = 0
        data_files = {}
        for root, _, files in os.walk(os.path.join(clueweb_root, 'txt')):
            # the dict will contain entries keyed by filename (without the .json.gz suffix),
            # and containing a tuple of (full filename, record count). the record counts are
            # initialized to zero here, they'll be populated in the next step
            data_files.update({
                f[:-8]: [os.path.join(root, f), 0]
                    for f in files if f.endswith('.json.gz')
            })
            if len(data_files) - last_length > 10000:
                print(f'> Files found: {len(data_files)}')
                last_length = len(data_files)

        print(f'> Built list of {len(data_files)} data files')

        # now find the record counts for each of the files. these are found under
        # <clueweb root>/record_counts/txt/. the path contains one CSV for each subdir
        # (e.g. "en00_counts.csv"). each CSV in turn has a line for each .json.gz that
        # the subdir contains, with the format:
        #   subdir_filenumber,recordcount
        # e.g.:
        #   en0046-86,18088
        for root, _, files in os.walk(os.path.join(clueweb_root, 'record_counts', 'txt')):
            for f in files:
                with open(os.path.join(root, f), 'r') as csvf:
                    for row in csv.reader(csvf):
                        file_id, records = row
                        if file_id not in data_files:
                            # should never happen for a valid copy of the dataset
                            print(f'WARNING: Could not find {file_id} in data files ({os.path.join(root, f)}')
                            continue
                        # set the record count for this data file
                        data_files[file_id][1] = records

        print('> Extracted all record counts')

        conn = sqlite3.connect(output_filename)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS files (
                            id INTEGER NOT NULL PRIMARY KEY,
                            path TEXT UNIQUE, 
                            records INTEGER, 
                            state INTEGER,
                            job TEXT, 
                            started TEXT, 
                            finished TEXT
                        )''')

        # insert the files in a single transaction, sorting them first. The sorting is
        # uesful because it ensures that the output files from the worker processes that
        # perform the scanning will also be sorted because files are retrieved from the
        # database from beginning to end. So even though there will be gaps in the record
        # numbering within a single worker output file, that set of records will be in
        # sorted order. This is useful because it avoids having to sort potentially very 
        # large CSV files later, and allow a final fully-sorted file to be generated using
        # clueweb_heap_sort.py
        sorted_data_file_keys = list(data_files.keys())
        sorted_data_file_keys.sort()

        cur.execute('BEGIN TRANSACTION')
        for key in sorted_data_file_keys:
            data_file_path, records = data_files[key]
            # each entry is inserted with the state set to NOT_STARTED and a blank start/end time
            cur.execute('INSERT INTO files VALUES (NULL, ?, ?, ?, NULL, NULL, NULL)', 
                            (data_file_path, records, ClueWebFileDatabase.NOT_STARTED ))
        cur.execute('COMMIT')
        conn.close()

        print(f'> Database with {len(data_files)} rows generated in {time.time() - start_time:.2f}s')

    def get_next_batch(self, job_id: str, count: int) -> Tuple[list[int], List[str]]:
        """
        Retrieve a new batch of filenames to be scanned. 

        This method returns some number of filenames from the database so they can
        be handed off to another process that will carry out a scan. The database is
        updated to mark the selected files as being IN_PROGRESS and the supplied <job_id>
        parameter is also added to the affected rows to make it easier to track which
        worker processses scanned which files.

        Files are returned in the order they were originally inserted, which is currently
        arbitrary. The return value is a 2-tuple containing a list of integer row IDs and
        a list of filenames. There will be <count> entries in the lists, unless there are
        insufficient remaining NOT_STARTED files to return. If the number of files remaining
        is less than <count>, all these files are returned. If there are no remaining files,
        the lists will be empty. 

        Args:
            job_id (str): an identifier for the worker processs that will scan this set of files
            count (int): the number of files to return

        Returns:
            tuple(list of row IDs, list of filenames)
        """
        batch_ids, batch_files = [], []

        try:
            cur = self.conn.cursor()
            cur.execute('BEGIN TRANSACTION')
            for row in cur.execute('SELECT id, path from files WHERE state = ? ORDER BY id ASC LIMIT ?', (ClueWebFileDatabase.NOT_STARTED, count)):
                batch_ids.append(str(row[0]))
                batch_files.append(row[1])

            cur.execute(f'UPDATE files SET state = ?, job = ? where id in (' + ','.join(batch_ids) + ')', (ClueWebFileDatabase.IN_PROGRESS, job_id))
            self.conn.commit()
        except sqlite3.OperationalError as oe:
            print(f'get_next_batch: Database error occurred: {str(oe)}')

        return batch_ids, batch_files

    def get_record_count_for_job(self, job_id: str) -> int:
        """
        Returns the total number of records assigned to a job ID.

        This method returns the sum of the record counts for all the data files which
        have been marked as assigned to the given job ID.

        Args:
            job_id (str): a job identifier

        Returns:
            the total number of records for the files assigned to the job
        """
        cur = self.conn.cursor()
        res = cur.execute('SELECT SUM(records) FROM files WHERE job = ?', (job_id,))
        return res.fetchone()[0]

    def clear_batch(self, job_id: str) -> bool:
        """
        Reset the 'state' of all files for a job ID to NOT_STARTED.

        This method can be used to clear the state in the database for a failed/incomplete
        job that needs to be run again. Given a job ID it will update the state of all associated
        data files to NOT_STARTED, and also clear the job ID column on these rows. This will 
        allow subsequent calls to get_next_batch to retrieve these files again. 

        Args:
            job_id (str): a job identifier

        Returns:
            True if successful, False if a database error occurred
        """
        try:
            cur = self.conn.cursor()
            cur.execute('UPDATE files SET state = ?, job = ? where job = ?', (ClueWebFileDatabase.NOT_STARTED, "", job_id))
            self.conn.commit()
        except sqlite3.OperationalError as oe:
            print(f'clear_batch: Database error occurred {str(oe)}')
            return False

        return True

    def complete_batch(self, job_id: str) -> bool:
        """
        Mark a set of files as having been successfully scanned.

        Given a job ID, update the state of all of its associated files to DONE
        to mark them as having been successfully scanned.

        Args:
            job_id (str): a job identifier

        Returns:
            True if successful, False if a database error occurred
        """
        try:
            cur = self.conn.cursor()
            cur.execute('UPDATE files SET state = ? where job = ?', (ClueWebFileDatabase.DONE, job_id))
            self.conn.commit()
        except sqlite3.OperationalError as oe:
            print(f'complete_batch: Database error occurred {str(oe)}')
            return False

        return True

    def complete_batch_files(self, files: List[str]) -> bool:
        """
        Mark a set of files as having been successfully scanned.

        Given a list of filenames, update the state of each to DONE to mark
        them as having been successfully scanned.

        Args:
            files (List[str]): a list of data file names (including paths)

        Returns:
            True if successful, False if a database error occurred
        """
        try:
            cur = self.conn.cursor()
            for path in files:
                cur.execute('UPDATE files SET state = ? where path = ?', (ClueWebFileDatabase.DONE, path))
            self.conn.commit()
        except sqlite3.OperationalError as oe:
            print(f'complete_batch_files: Database error occurred {str(oe)}')
            return False

        return True

    def check_progress(self) -> Tuple[int, int]:
        """
        Return the number of completed files and the total number of files.

        This method returns a tuple containing the number of files which have been
        marked as scanned (state == DONE) and the total number of files, to allow
        for simple progress calculation. 

        Returns:
            tuple(number of scanned files, total number of files)
        """

        cur = self.conn.cursor()
        r1 = cur.execute('SELECT COUNT(state) FROM files WHERE state == ?', (ClueWebFileDatabase.DONE, ))
        files_scanned = r1.fetchone()[0]
        r2 = cur.execute('SELECT COUNT(state) FROM files')
        files_total = r2.fetchone()[0]
        return (files_scanned, files_total)

    def close(self) -> None:
        """
        Close the database connection handle.

        Return:
            None
        """
        self.conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--clueweb_root', help='Path to ClueWeb dataset root directory', required=True, type=str)
    parser.add_argument('-o', '--output_filename', help='Path for the generated database', required=True, type=str)
    args = parser.parse_args()
    ClueWebFileDatabase.generate(args.clueweb_root, args.output_filename)
