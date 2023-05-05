import argparse
import logging
import time

import zmq

from clueweb_zmq import (
    ZMSG_ACK,
    ZMSG_FINISHED,
    ZMSG_LOCAL_EXIT,
    ZMSG_LOCAL_RESET_JOB,
    ZMSG_NEWJOB,
)
from clueweb_dbwrapper import ClueWebFileDatabase

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler('coordinator.log'),
            #logging.StreamHandler()
    ])

class ClueWebCoordinator:
    """
    This class runs a 'coordinator' process to manage an unknown number of worker jobs
    performing a full scan of an instance of the ClueWeb22 dataset. 

    It uses an instance of ClueWebFileDatabase from the clueweb_dbwrapper module to 
    manage job state (the database should be created separately by running the _dbwrapper
    module directly), and relies on ZeroMQ for communication with the worker processes. 

    When a worker is started using the clueweb_metadata_scanner.py module, it will contact the 
    coordinator and request a set of files to process. The coordinator will retrieve a batch
    of files from the database, mark them as being processed by the current worker, and then send
    the filenames back to the worker process.

    This should work on a single machine, but is intended for use on a cluster due to the
    size of ClueWeb22_L.

    In addition to communication with worker processes, the coordinator also supports commands
    sent interactively through a separate script (see clueweb_ctrl.py). This functionality can
    be used to tell the coordinator process to exit, or to reset the state for a worker that
    encountered a problem. 
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.db = ClueWebFileDatabase(args.database)

    def run(self):
        # set up the pair of ZMQ sockets for worker and control messages
        zmq_ctx_jobs = zmq.Context()
        zmq_sock_jobs = zmq_ctx_jobs.socket(zmq.REP)
        zmq_ctx_ctrl = zmq.Context()
        zmq_sock_ctrl = zmq_ctx_ctrl.socket(zmq.REP)
        zmq_sock_jobs.bind(f'tcp://*:{self.args.port}')
        zmq_sock_ctrl.bind(f'tcp://*:{self.args.ctrl_port}')
        logging.info(f'Listening on ZMQ ports {self.args.port}, {self.args.ctrl_port}')

        done = False
        while not done:
            msgtype, msgdata = None, None

            # check for job messages first, using the DONTWAIT flag so we don't block
            try:
                msgtype, msgdata = zmq_sock_jobs.recv_pyobj(zmq.DONTWAIT)
            except zmq.Again:
                pass

            if msgtype is None:
                # if there were no job messages, check for any control messages 
                # in the same way
                try:
                    msgtype, msgdata = zmq_sock_ctrl.recv_pyobj(zmq.DONTWAIT)
                except zmq.Again:
                    # no messages from either socket, wait a little and then try again
                    time.sleep(0.5)
                    continue

            reply_type, reply_data = ZMSG_ACK, None
            if msgtype == ZMSG_NEWJOB:
                # a new worker process is requesting a set of files to scan.
                # msgdata should contain the remote job name + number of files it wants to process
                remote_job, num_files = msgdata
                logging.info(f'Received a request for {num_files} files from {remote_job}')
                _, next_batch = self.db.get_next_batch(remote_job, num_files)
                logging.info(f'Returning {len(next_batch)} jobs to {remote_job}')
                reply_data = next_batch
            elif msgtype == ZMSG_FINISHED:
                # a worker process has finished scanning its files. msgdata should
                # contain the job name, number of files scanned, and a True/False overall result
                remote_job, num_files, result = msgdata
                if result:
                    logging.info(f'[{remote_job}] finished processing {num_files}, updating database')
                    self.db.complete_batch(remote_job)
                else:
                    logging.error(f'[{remote_job}] failed to complete successfully!')
            elif msgtype == ZMSG_LOCAL_EXIT:
                # control message telling the coordinator to exit
                logging.warning('Coordinator received exit message, will exit')
                done = True
            elif msgtype == ZMSG_LOCAL_RESET_JOB:
                # control messages telling the coordinator to reset the database state
                # for the files associated with the indicated job ID
                remote_job, = msgdata
                result = self.db.clear_batch(remote_job)
                logging.warning(f'Clearing state for job {remote_job}, result={result}')
            else:
                logging.warning(f'Unknown message type {msgtype}')

            zmq_sock_jobs.send_pyobj((reply_type, reply_data))
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', '--database', help='Database file location', required=True, type=str)
    parser.add_argument('-p', '--port', help='ZMQ port number to listen on for job messages', type=int, default=23456)
    parser.add_argument('-P', '--ctrl_port', help='ZMQ port number to listen on for control messages', type=int, default=23457)

    args = parser.parse_args()

    ClueWebCoordinator(args).run()
