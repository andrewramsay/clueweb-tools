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

from clueweb_dbwrapper import ClueWebFileDatabase
from clueweb_zmq import ZMSG_ACK, ZMSG_LOCAL_PAUSE_WORKER, ZMSG_LOCAL_RESUME_WORKER
from utils import fmt_timespan

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=logging.INFO,
        handlers=[
            # logging.FileHandler('scanner.log'),
            logging.StreamHandler()
    ])

MSG_FINISHED       = 0 # sent by worker when process is existing
MSG_PROGRESS       = 1 # sent by worker to update parent process on progress
MSG_NEW_FILE       = 2 # sent by parent to worker to provide it with a new file
MSG_FILE_SCANNED   = 3 # sent by worker to parent to report a file was scanned
MSG_NOFILES        = 4 # sent by parent to worker if no files remain to be processed
MSG_PAUSE          = 5 # sent by parent to worker to tell it to stop (but not exit)

class ClueWebMetadataScannerDynamic:
    """
    This class is a combination of the clueweb_metadata_scanner and clueweb_metadata_coordinator
    modules that allows for more control over the I/O load that is generated by the scanning
    process by varying the number of active workers in response to ZMQ messages sent interactively. 
    
    The key differences between this class and the clueweb_metadata_scanner/coordinator modules are:
        - this module is intended to run a single instance on a compute node
        - the number of cores given on the command-line is a maximum limit and processes can be
            paused and resumed by sending ZMQ messages through a separate script

    The result should be no different to using the alternative scripts, but might be a better fit 
    for some situations.
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.job_id = 'clueweb_metadata_scanner'
        self.start_time = None
        self.core_count = args.procs
        self.db = ClueWebFileDatabase(args.database)
        self.worker_procs = []
        # each worker will start in the "paused" state
        self.workers_active = {i: False for i in range(args.procs)}
        self.pipes = {}
        self.done = False
        self.zmq_ctx = zmq.Context()
        self.zmq_sock = self.zmq_ctx.socket(zmq.REP)
        self.zmq_sock.bind(f'tcp://*:{args.control_port}')

    def log(self, msg: str) -> None:
        """
        Helper method for logging messages with a job prefix.

        Args:
            msg (str): the log message text

        Returns:
            None
        """
        logging.info(f'[{self.job_id}] {msg}')

    def gather_metadata(self, worker_id: int, pipew: Connection) -> None:
        """
        Scan multiple ClueWeb .json.gz text files, extracting metadata to a new CSV file.

        This is the target method for the multiprocessing.Process objects spawned by
        the main process. Each instance enters a loop of:
            - requesting a new file from the parent process
            - iterating through each record in the file (.json.gz format)
            - extracting all metadata fields 
            - writing these to a per-process .csv file
        
        This continues until no more files are available to be scanned.

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
        output_csv = os.path.join(self.args.output, self.job_id + f'-worker{worker_id}.csv')
        os.makedirs(args.output, exist_ok=True)
        if os.path.exists(output_csv):
            raise Exception(f'Output file {output_csv} already exists!')

        with open(output_csv, 'w') as csvfile:
            csvwriter = csv.writer(csvfile, lineterminator='\n')

            while True:
                # wait for messages from the parent process. workers start in a paused state
                # so they will block here until the parent process tells them to start doing
                # some work
                msgtype, msgdata = pipew.recv()

                if msgtype == MSG_PAUSE:
                    logging.info(f'[{self.job_id}-worker{worker_id}] is in paused state')
                    continue

                # if we reach here, the worker is currently active and msgdata 
                # should contain a new file path to scan. however need to check if
                # we've run out of files...
                if msgtype == MSG_NOFILES:
                    # we're out of files to process, worker should exit
                    logging.info(f'[{self.job_id}-worker{worker_id}] has no files to process, exiting')
                    break

                # at this point msgdata should contain a valid filename to be scanned
                try:
                    logging.info(f'[{self.job_id}-worker{worker_id}] will scan file{msgdata}')
                    records = []
                    fp = open(msgdata, 'rb')
                    # file format details: https://lemurproject.org/clueweb22/docspecs.php#txt
                    with gzip.GzipFile(filename=None, mode='rb', fileobj=fp) as gp:
                        with jsonlines.Reader(gp) as reader:
                            for record in reader:
                                records.append((record['ClueWeb22-ID'], 
                                                record['URL'].strip(),  # URLs all seem to have a trailing newline
                                                record['URL-hash'], 
                                                record['Language']))

                    for r in records:
                        csvwriter.writerow(r)

                    total_records += len(records)
                    total_files += 1
                    logging.info(f'[{self.job_id}-worker{worker_id}] scanned file{msgdata}, {len(records)} records')
                    # tell the parent process we've finished this file successfully
                    pipew.send((worker_id, MSG_FILE_SCANNED, msgdata))
                except Exception as e:
                    logging.error(f'[{self.job_id}-worker{worker_id}] encountered an error: {str(e)} on file {msgdata}')
                    completed_ok = False
                    time.sleep(1)
                    # report an error
                    pipew.send((worker_id, MSG_FILE_SCANNED, None))
                    continue

                if total_files > 0 and total_files % 25 == 0:
                    pipew.send((worker_id, MSG_PROGRESS, total_files))

        # confirm to the parent process we've exited
        pipew.send((worker_id, MSG_FINISHED, completed_ok))

    def num_active(self) -> int:
        """
        Get the number of currently active worker processes.

        This will return the number of Process objects that are currently
        scanning files, i.e. not in the "paused" state.

        Returns:
            number of active worker processes
        """
        return list(self.workers_active.values()).count(True)

    def send_file_to_worker(self, worker_id: int, pipe: Connection) -> bool:
        """
        Retrieves the next available file and sends it to the indicated worker.

        Args:
            worker_id (int): worker index
            pipe (Connection): a Pipe to communicate with the worker

        Returns:
            True if a new file was sent, False if no files remain
        """
        db_id = f'{self.job_id}-worker{worker_id}'
        _, next_files = self.db.get_next_batch(db_id, 1)
        if len(next_files) == 0:
            logging.info(f'[{self.job_id}] worker {worker_id} requested a file, but none remain')
            pipe.send((MSG_NOFILES, None))
            return False

        pipe.send((MSG_NEW_FILE, next_files[0]))
        return True

    def run(self) -> None:
        """
        Spawn a set of parallel worker processes to scan ClueWeb data files.

        This method manages the set of worker processes on the local machine. It starts
        each process in a "paused" state, and then waits for messages sent from a separate
        script over a ZMQ socket to tell it which workers should be started up. 

        The number of workers can be varied interactively while the main process is active.

        An instance of ClueWebFileDatabase is used to manage the scanning progress and track
        available files.
        
        Returns:
            None
        """
        self.log(f'starting at {datetime.datetime.now().isoformat()}')

        current_file = 0
        num_finished = 0

        # set up worker processes, passing in one end of a Pipe to allow for communication
        for i in range(self.core_count):
            p1, p2 = multiprocessing.Pipe()
            self.pipes[i] = (p1, p2)
            self.worker_procs.append(multiprocessing.Process(target=self.gather_metadata, args=(i, p2)))

        self.start_time = time.time()
        self.log(f'Starting {len(self.worker_procs)} workers')
        for p in self.worker_procs:
            p.start()

        pipes = [p[0] for p in self.pipes.values()]

        try:
            while not self.done:
               # first check for external control messages over the ZMQ socket
                try:
                    (msgtype, msgdata) = self.zmq_sock.recv_pyobj(zmq.DONTWAIT)

                    if msgtype == ZMSG_LOCAL_RESUME_WORKER:
                        worker_id = msgdata
                        if worker_id >= len(self.worker_procs):
                            logging.error(f'Invalid worker ID: {worker_id}')
                        else:
                            # mark the worker as active, then send it a file to wake it up
                            self.workers_active[worker_id] = True
                            self.log(f'Resuming worker {worker_id} (active={self.num_active()})')
                            self.log(f'Active workers: {[k for k, v in self.workers_active.items() if v]}')
                            if self.send_file_to_worker(worker_id, pipes[worker_id]):
                                current_file += 1
                    elif msgtype == ZMSG_LOCAL_PAUSE_WORKER:
                        worker_id = msgdata
                        if worker_id >= len(self.worker_procs):
                            logging.error(f'Invalid worker ID: {worker_id}')
                        else:
                            # mark the worker as inactive. the next time the worker requests a file
                            # it will receive a MSG_PAUSE and enter the paused state (if it's currently
                            # processing a file, that will not be interrupted)
                            self.workers_active[worker_id] = False
                            self.log(f'Pausing worker {worker_id} (active={self.num_active()})')
                            self.log(f'Active workers: {[k for k, v in self.workers_active.items() if v]}')
                    self.zmq_sock.send_pyobj((ZMSG_ACK, None))
                except zmq.Again:
                    # if there are no ZMQ messages waiting, fall through to check for messages
                    # from the worker processes
                    pass

                # wait for a worker to request a new batch of files. this call blocks
                # until a Pipe becomes ready for reading or the timeout expires
                ready_to_read = multiprocessing.connection.wait(pipes, timeout=0.1)

                for pipe in ready_to_read:
                    data = pipe.recv()

                    if data is None or not isinstance(data, tuple):
                        logging.warning(f'[{self.job_id}] failed to receive anything from pipe')
                        continue

                    worker_id, msgtype, content = data
                    if msgtype == MSG_FINISHED:
                        # a worker process finished
                        num_finished += 1
                        logging.info(f'[{self.job_id}] worker {worker_id} is finished with result {content} ({num_finished}/{len(self.worker_procs)})')
                    elif msgtype == MSG_PROGRESS:
                        # progress updates
                        num_files = content
                        logging.info(f'[{self.job_id}-worker{worker_id}] has scanned {num_files} files')
                    elif msgtype == MSG_FILE_SCANNED:
                        if content is not None:
                            # record file was scanned successfully by the worker
                            self.db.complete_batch_files([content])

                        # the worker will also be requesting a new file, but first have to check if it should be paused
                        if not self.workers_active[worker_id]:
                            self.log(f'Worker {worker_id} is requesting a file, but has been paused')
                            pipe.send((MSG_PAUSE, None))
                        else:
                            # if it's still active, give it a new file (if any remain)
                            if self.send_file_to_worker(worker_id, pipes[worker_id]):
                                current_file += 1

                            if current_file > 0 and current_file % 25 == 0:
                                elapsed = time.time() - self.start_time
                                files_per_minute = current_file / (elapsed / 60.0)
                                logging.info(f'[{self.job_id}] has scanned {current_file} files in {fmt_timespan(elapsed)}, files/min={files_per_minute:.1f}')

            logging.info(f'[{self.job_id}] All processes finished, joining...')
            for p in self.worker_procs:
                p.join()
        except Exception as e:
            logging.error(f'[{self.job_id}] encountered an error: {str(e)}, results may be incomplete!')

        logging.info(f'[{self.job_id}] Total time: {fmt_timespan(time.time() - self.start_time)}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='Database file location', required=True, type=str)
    parser.add_argument('-o', '--output', help='Output .csv path', required=True, type=str)
    parser.add_argument('-p', '--procs', help='Number of cores to use', required=True, type=int)
    parser.add_argument('-P', '--control_port', help='port for ZMQ messages', default=23456, type=int)

    args = parser.parse_args()
    ClueWebMetadataScannerDynamic(args).run()
