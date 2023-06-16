import sys
import os
import sqlite3
import csv

import tqdm

# TODO lots of work needed to tidy this up!

sys.path.append('/mnt/archive/ClueWeb22_L/csvmonkey')
import csvmonkey

if __name__ == "__main__":
    conn = sqlite3.connect('/mnt/860evo/clueweb_A_urls.db', isolation_level=None)
    conn.execute('PRAGMA cache_size = -20000000')
    conn.execute('PRAGMA journal_mode = OFF')
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA temp_store = MEMORY')
    include = [f'ClueWeb22_A_ID_URL_index{x:02d}.csv' for x in range(2, 27, 1)]

    conn.execute('CREATE TABLE IF NOT EXISTS urls (id INTEGER PRIMARY KEY, clueweb_id TEXT UNIQUE, url TEXT)')
    root = '/mnt/archive/ClueWeb22_A/uncompressed'
    pb = tqdm.tqdm(total=2_000_000_000)
    for f in include:
        print('Current file:', f)
        batch = []
        lines = 0
        conn.execute('BEGIN TRANSACTION')
        started = False
        for row in csvmonkey.from_path(os.path.join(root, f), header=False, delimiter=b',', yields='tuple'):
            cwid, url, _, lang = row
            if lang == 'de':
                lines += 1
                if lines % 10000 == 0:
                    pb.update(10000)
                continue
            if not started:
                started = True
                print('> Beginning to insert')
            if lang != 'en':
                print('Found end of English records')
                if len(batch) > 0:
                    try:
                        conn.executemany('INSERT INTO urls VALUES (NULL, ?, ?)', batch)
                    except sqlite3.IntegrityError as ie:
                        raise ie
                break

            batch.append((cwid[10:], url))

            if len(batch) == 100000:
                try:
                    conn.executemany('INSERT INTO urls VALUES (NULL, ?, ?)', batch)
                except sqlite3.IntegrityError as ie:
                    print(batch)
                    raise ie
                lines += len(batch)
                pb.update(len(batch))

                batch = []

        conn.commit()

    pb.close()
    conn.close()
