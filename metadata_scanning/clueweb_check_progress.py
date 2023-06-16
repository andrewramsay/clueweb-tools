import argparse
import sqlite3

from clueweb_dbwrapper import ClueWebFileDatabase

"""
This script provdes a simple way to check the progress of an ongoing 
metadata extraction by querying the number of completed files from the 
local database.
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='Path to database file', required=True, type=str)
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    cur = conn.cursor()
    res =  cur.execute('SELECT COUNT(state) FROM files WHERE state == ?', (ClueWebFileDatabase.DONE, ))
    completed = res.fetchone()[0]
    res  = cur.execute('SELECT COUNT(state) FROM files')
    total = res.fetchone()[0]
    print(f'Completed {completed}/{total}, {100 * (completed/total):.2f}%')
