import os
import argparse
import sys

sys.path.append(os.path.dirname(os.path.split(os.path.abspath(__file__))[0]))
from misc.utils import sort_csv_parallel_sh

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', help='path to folder of unsorted CSV files', required=True, type=str)
    parser.add_argument('-d', '--destination', help='path to output folder', required=True, type=str)
    parser.add_argument('-c', '--cores', help='number of cores for "sort" to use', default=8, type=int)
    parser.add_argument('-b', '--buffersize', help='size of "sort" memory buffer, gigabytes', default=10, type=int)
    args = parser.parse_args()

    sort_csv_parallel_sh(args.source, args.destination, args.cores, args.buffersize)
