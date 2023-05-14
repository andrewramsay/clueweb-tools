import argparse
import logging
import os
import sqlite3

from clueweb_dbwrapper import ClueWebFileDatabase

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler('counter.log'),
            #logging.StreamHandler()
    ])

class ClueWebCounter:
    """
    This class might be useful for verifying results of a large scan of ClueWeb22_L.

    Given a database created by clueweb_dbwrapper.py containing lists of .json.gz
    data files and their expected record counts, and a path to a folder of .csv files
    created by one of the clueweb_metadata_scanner modules, it will do a simple count
    of the number of lines in each file and check that this matches the number of 
    records assigned to that job in the database. 

    As a convenience it also generates a new SQLite database with a single table that
    contains (filename, record_count) columns so that results can be easily examined. 
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.db = ClueWebFileDatabase(args.database)
        self.results_path = args.results

        self.count_db = sqlite3.connect(args.counts_database)
        self.count_db.execute('CREATE TABLE IF NOT EXISTS counts (filename TEXT UNIQUE, count INTEGER)')
        # avoid rescanning files if the script is run repeatedly
        self.already_counted = {}
        for row in self.count_db.execute('SELECT filename, count FROM counts'):
            self.already_counted[row[0]] = row[1]

    def count_lines(self, filename, bufsz=1024*1024*8):
        """
        A simple line counting method.

        Args:
            filename (str): the filename to count lines in
            bufsz (int): read buffer size in bytes

        Returns:
            number of lines counted
        """
        if os.path.split(filename)[1] in self.already_counted:
            return self.already_counted[os.path.split(filename)[1]]

        # open the file and set buffering=0 to bypass the builtin buffer handling
        fp = open(filename, 'rb', buffering=0)
        print(f'Counting lines in {filename}')

        lines = 0
        i = 0

        # read buffers of <bufsz> bytes until we run out of data
        buf = fp.read(bufsz)
        while len(buf) > 0:
            lines += buf.count(b'\n')
            buf = fp.read(bufsz)
            i += 1

            if i % 200 == 0:
                print(f'{filename}: {(i * bufsz)/(1024**3):.3f}GB')

        print(f'{filename} has {lines} lines')
        self.count_db.execute('INSERT INTO counts VALUES (?, ?)', (os.path.split(filename)[1], lines))
        self.count_db.commit()
        return lines

    def run(self) -> None:
        """
        Check record counts in scanner job outputs against expected record counts.

        This method iterates over a set of .csv outputs from one of the 
        clueweb_metadata_scanner modules, retrieves the number of records assigned
        to each job from a ClueWebFileDatabase instance, then counts the number of
        lines (=records) actually found in the corresponding .csv file. 

        Returns:
            None
        """

        # this expects to find a single folder of .csv files named:
        #    <job_id>.csv
        csv_files = {}
        for root, _, files in os.walk(self.results_path):        
            for f in files:
                if f.endswith('.csv'):
                    job_id = os.path.splitext(f)[0]
                    csv_files[job_id] = os.path.join(root, f)
            
        for job_name, job_results_file in csv_files.items(): 
            # this is the total number of records across all files processed by this job
            db_count = self.db.get_record_count_for_job(job_name)

            # count the number of lines in each file and sum 
            file_count = self.count_lines(job_results_file)

            # verify things match up with the database counts (these are taken 
            # originally taken from the ClueWeb record_counts files)
            if file_count > 0:
                if db_count == file_count:
                    logging.info(f'{job_name}: DB={db_count}, files={file_count}')
                else:
                    logging.warning(f'{job_name}: DB={db_count}, files={file_count} ***')
            else:
                logging.info(f'{job_name}: skipping empty file')
            
      
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', '--database', help='Database file (as created by clueweb_dbwrapper.py)', required=True, type=str)
    parser.add_argument('-c', '--counts_database', help='Location to create record counts database', type=str, default='counts.db')
    parser.add_argument('-r', '--results', help='Results location (.csv outputs from scanner processes)', required=True, type=str)

    args = parser.parse_args()

    ClueWebCounter(args).run()
