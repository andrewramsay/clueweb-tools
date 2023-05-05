import sys
import argparse

import zmq

from clueweb_zmq import ZMSG_LOCAL_EXIT, ZMSG_LOCAL_RESET_JOB

"""
This script allows some basic commands to be sent to an instance
of the clueweb_metadata_coordinator script. It can be used to tell
the coordinator process to exit, or to reset the database state
for a selected job ID (e.g. if it fails partway through). 
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='remote IP/hostname', required=True)
    parser.add_argument('-p', '--port', default=23457, type=int, help='remote port number')
    parser.add_argument('-x', '--exit', action='store_true')
    parser.add_argument('-r', '--reset_job', type=int, help='Reset database state for this job ID')
    args = parser.parse_args()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.connect(f'tcp://{sys.argv[1]}:23457')

    if args.exit:
        sock.send_pyobj((ZMSG_LOCAL_EXIT, None))
        sock.recv_pyobj()
    else:
        print(f'Reset on job {args.reset_job}')
        sock.send_pyobj((ZMSG_LOCAL_RESET_JOB, args.reset_job))
        sock.recv_pyobj()
