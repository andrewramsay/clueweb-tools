import argparse
import sys

import zmq

from clueweb_zmq import ZMSG_LOCAL_EXIT, ZMSG_LOCAL_PAUSE_WORKER, ZMSG_LOCAL_RESET_JOB, ZMSG_LOCAL_RESUME_WORKER

"""
This script is used to control the state of the worker processes when running an
instance of clueweb_metadata_scanner_dynamic.py. 

For example, to pause the first worker process:

$ python clueweb_ctrl.py -a <remote_address> -P 0

To resume it again:

$ python clueweb_ctrl.py -a <remote_address> -R 0
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='remote IP/hostname', required=True)
    parser.add_argument('-p', '--port', default=23456, type=int, help='remote port number')
    parser.add_argument('-P', '--pause', type=int, help='Worker index (0-n)')
    parser.add_argument('-R', '--resume', type=int, help='Worker index (0-n)')
    args = parser.parse_args()

    if (args.resume is None and args.pause is None) or (args.resume is not None and args.pause is not None):
        print('Must give exactly one of --resume/--pause')
        sys.exit(0)

    # create a ZMQ socket to talk to the remote process
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.connect(f'tcp://{args.address}:{args.port}')

    # send a pause/resume message for the given worker ID
    msgtype = ZMSG_LOCAL_RESUME_WORKER if args.resume is not None else ZMSG_LOCAL_PAUSE_WORKER
    msgdata = args.resume if args.resume is not None else args.pause
    if msgtype == ZMSG_LOCAL_RESUME_WORKER:
        print(f'Enabling worker {msgdata}')
    elif msgtype == ZMSG_LOCAL_PAUSE_WORKER:
        print(f'Disabling worker {msgdata}')
    sock.send_pyobj((msgtype, msgdata))
    sock.recv_pyobj()
