import argparse
import csv
import datetime
import gzip
import logging
import multiprocessing
import multiprocessing.connection
import os
import time
from multiprocessing.connection import Connection

import jsonlines
import zmq

from clueweb_zmq import (
    ZMSG_ACK,
    ZMSG_FINISHED,
    ZMSG_NEWJOB,
)
from utils import fmt_timespan

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=logging.INFO,
        handlers=[
            # logging.FileHandler('scanner.log'),
            logging.StreamHandler()
    ])

MSG_FINISHED = 0 # sent by worker when process is existing
MSG_PROGRESS = 1 # sent by worker to update parent process on progress
MSG_NEWFILE  = 2 # sent by worker to request a new file to process (and by parent to respond)
MSG_NOFILES  = 3 # sent by parent to worker if no files remain to be processed

class ClueWebMetadataScanner:
    """
    This class is intended to be used with the clueweb_coordinator module to run
    multiple parallel jobs each scanning a different set of ClueWeb22_L data files. 

    A job instance is given a number of files to process, a number of processes to spawn,
    and a hostname:port to contact the coordinator on via ZeroMQ, plus an output CSV filename.

    The coordinator will return a list of file paths to scan. These files are
    then distributed among the parallel worker processes, with each process scanning a complete
    file, returning the results, and then requesting a new file.
    (see clueweb_metadata_scanner_dynamic.py for a better implementation)
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.job_id = args.name
        self.start_time = None
        self.num_files = args.count
        self.core_count = args.procs
        # ZMQ socket for communication with the coordinator process
        self.zmq_ctx = zmq.Context()
        self.zmq_sock = self.zmq_ctx.socket(zmq.REQ)

    def gather_metadata(self, worker_id: int, pipew: Connection) -> None:
        """
        Scan a single ClueWeb .json.gz text file and extract metadata to a new CSV file.

        This is the target method for the multiprocessing.Process objects spawned by
        the main process. Each instance iterates through each record in a ClueWeb text 
        format .json.gz file, extracting all fields except the full text and writing them 
        to a new per-process CSV file.

        Args:
            worker_id (int): index of this worker in the process pool
            pipew (Connection): a Pipe for passing messages back to the parent process

        Returns:
            None
        """
        total_records = 0
        total_files = 0
        completed_ok = True
        # create an output path based on the worker ID so we get 1 CSV file for each spawned process
        output_csv = os.path.join(self.args.output, self.job_id + f'-w{worker_id}.csv')
        os.makedirs(args.output, exist_ok=True)

        with open(output_csv, 'w') as csvfile:
            csvwriter = csv.writer(csvfile, lineterminator='\n')

            while True:
                # loop around requesting files to process until none are left
                pipew.send((worker_id, MSG_NEWFILE, None))
                msgtype, data_file = pipew.recv()

                if msgtype == MSG_NOFILES:
                    # we're out of files to process, the worker should now exit
                    logging.info(f'[{self.job_id}-worker{worker_id}] has no files to process, exiting')
                    break

                try:
                    records = []
                    fp = open(data_file, 'rb')
                    # file format details: https://lemurproject.org/clueweb22/docspecs.php#txt
                    with gzip.GzipFile(filename=None, mode='rb', fileobj=fp) as gp:
                        with jsonlines.Reader(gp) as reader:
                            for record in reader:
                                records.append((record['ClueWeb22-ID'], 
                                                record['URL'].strip(),  # URLs all seem to have a trailing newline
                                                record['URL-hash'], 
                                                record['Language']))

                    # TODO: could just do this as we read the rows
                    for r in records:
                        csvwriter.writerow(r)

                    total_records += len(records)
                    total_files += 1
                except Exception as e:
                    logging.error(f'[{self.job_id}-worker{worker_id}] encountered an error: {str(e)} on file {data_file}')
                    completed_ok = False
                    break

                # send a progress update to the main process every 25 files
                if total_files > 0 and total_files % 25 == 0:
                    pipew.send((worker_id, MSG_PROGRESS, total_files))

        # confirm that this worker is exiting
        pipew.send((worker_id, MSG_FINISHED, completed_ok))

    def run(self) -> None:
        """
        Spawn a set of parallel worker processes to scan ClueWeb data files.

        This method handles the communication with the coordinator instance as well as
        managing the worker processes. Initially it will request a set of files from the
        coordinator based on the CLI parameters, then it starts a set of Processes, and
        begins handing out files to them until the list is exhausted. Once all files and
        Processes are complete, it reports back to the coordinator so that that batch of 
        files can be marked as having been successfully scanned.
        
        Returns:
            None
        """
        logging.info(f'[{self.job_id}] starting at {datetime.datetime.now().isoformat()}')
        try:
            self.zmq_sock.connect(f'tcp://{self.args.remote_address}:{self.args.remote_port}')
        except Exception as e:
            logging.error(f'[{self.job_id}] failed to connect to coordinator: {str(e)}')
            return

        # send a message to the coordinator process, asking it to return a given
        # number of files for us to process
        self.zmq_sock.send_pyobj((ZMSG_NEWJOB, (self.job_id, self.args.count)))
        reply_type, data_files = self.zmq_sock.recv_pyobj()

        if reply_type != ZMSG_ACK or data_files is None:
            logging.info(f'[{self.job_id}] Negative reply from coordinator, exiting')
            return

        logging.info(f'[{self.job_id}] Retrieved {len(data_files)} files')
        pipes = {}
        processes = []
        current_file = 0
        num_finished = 0
        num_successful = 0

        # set up worker processes, passing in one end of a Pipe to each to allow for simple communication
        for i in range(self.core_count):
            pr, pw = multiprocessing.Pipe()
            pipes[i] = (pr, pw)
            processes.append(multiprocessing.Process(target=self.gather_metadata, args=(i, pw)))

        self.start_time = time.time()
        logging.info(f'[{self.job_id}] Starting {len(processes)} workers')
        for p in processes:
            p.start()

        pipe_readers = [p[0] for p in pipes.values()]

        try:
            while num_finished < len(processes):
                # wait for a worker to request a new file. this call will return
                # if any of the Pipes have data waiting, or if the timeout expires
                ready_to_read = multiprocessing.connection.wait(pipe_readers, timeout=0.1)

                # if we didn't time out, read from the Pipe(s) with available data
                for reader in ready_to_read:
                    data = reader.recv()

                    if data is None or not isinstance(data, tuple):
                        logging.warning(f'[{self.job_id}] failed to receive anything from pipe')
                        continue

                    worker_id, msgtype, content = data
                    if msgtype == MSG_FINISHED:
                        # a worker process has exited (usually after we run out of files to scan)
                        num_finished += 1
                        if content:
                            num_successful += 1
                        logging.info(f'[{self.job_id}] worker {worker_id} is finished with result {content} ({num_finished}/{len(processes)})')
                    elif msgtype == MSG_PROGRESS:
                        # worker process sending a progress update
                        num_files = content
                        logging.info(f'[{self.job_id}-worker{worker_id}] has scanned {num_files} files')
                    elif msgtype == MSG_NEWFILE:
                        # worker process requesting a new file to scan
                        if current_file >= len(data_files):
                            # no more files to scan, return MSG_NOFILES to tell worker process to exit
                            logging.info(f'[{self.job_id}] worker {worker_id} requested a file, but none remain')
                            reader.send((MSG_NOFILES, None))
                        else:
                            # we still have files to scan, return the next available one
                            # logging.info(f'[{self.job_id}] sending file {current_file} to worker {worker_id}')
                            reader.send((MSG_NEWFILE, data_files[current_file]))
                            current_file += 1

                        if current_file > 0 and current_file % 25 == 0:
                            # log some simple progress information
                            elapsed = time.time() - self.start_time
                            remaining = (len(data_files) - current_file) / (current_file / elapsed)
                            percent_complete = 100 * (current_file / len(data_files))
                            logging.info(f'[{self.job_id}] has scanned {current_file} files ({percent_complete:.1f}%), ETC={fmt_timespan(remaining)}')

            logging.info(f'[{self.job_id}] All processes finished, joining...')
            for p in processes:
                p.join()
        except Exception as e:
            logging.error(f'[{self.job_id}] encountered an error: {str(e)}, results may be incomplete!')

        logging.info(f'[{self.job_id}] Total time: {fmt_timespan(time.time() - self.start_time)}')
        
        # notify coordinator we're done so it can mark the batch as completed
        self.zmq_sock.send_pyobj((ZMSG_FINISHED, (self.job_id, self.num_files, num_successful == len(processes))))
        self.zmq_sock.recv_pyobj()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--count', help='Number of new ClueWeb .json.gz files to scan', required=True, type=int)
    parser.add_argument('-o', '--output', help='Output .csv path', required=True, type=str)
    parser.add_argument('-p', '--procs', help='Number of cores to use', required=True, type=int)
    parser.add_argument('-n', '--name', help='Job name', required=True, type=str)
    parser.add_argument('-r', '--remote_address', help='IP/hostname of the system running the coordinator process', required=True, type=str)
    parser.add_argument('-P', '--remote_port', help='coordinator ZMQ port', type=int, default=23456)

    args = parser.parse_args()
    ClueWebMetadataScanner(args).run()
