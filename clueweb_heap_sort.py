import heapq
import os
import time
import argparse

from utils import fmt_timespan

class File:
    """
    Simple wrapper for reading lines from a file 1-by-1.
    """

    def __init__(self, path: str) -> None:
        self.fp = open(path, 'r')
        self.name = os.path.split(path)[1]
        self.lines_read = 0
        self.exhausted = False

    def read_line(self) -> None | str:
        """
        Returns the next line from the file, or None if at EOF.

        Returns:
            either a string containing the next line, or None if no more lines
        """
        if self.exhausted:
            return None

        next_line = self.fp.readline()
        if len(next_line) == 0:
            self.exhausted = True
            return None

        return next_line

    def close(self) -> None:
        self.fp.close()

    def __repr__(self) -> str:
        return self.name

def run(args: argparse.Namespace) -> None:
    """
    Use a min-heap to merge sort a collection of already-sorted input files.

    Due to the size of the ClueWeb22_L collection, there was no realistic way
    to sort a CSV file containing metadata for every record in memory. To get
    a fully sorted single file, a workaround is to sort the individual output
    files from the scanning process, then use a min-heap structure to read lines
    from each file one at a time and write them into the output file as we go
    along. 

    The algorithm is very simple:
        populate a min-heap with a line from each file
        while lines left
            pop next line from the heap and write to output
            read a new line from the file that line came from (if possible)

    Note that this only works if the input files are already sorted in the 
    same order that the output file should be!

    See:
       - https://en.wikipedia.org/wiki/Binary_heap
       - https://docs.python.org/3/library/heapq.html
    """

    # open all our (already sorted) input files
    sorted_files = {}
    heap = []

    # filter out anything without the expected extension
    for f in os.listdir(args.input):
        if not f.endswith(".csv.sorted"):
            continue

        sorted_files[f] = File(os.path.join(args.input, f))

    print(f'Opened {len(sorted_files)} files')

    output_file = open(args.output, 'w')

    # Python's heapq acts as a min-heap by default. So the process here is:
    #  - start by reading the first line from every file and adding to the heap
    #  - pop the heap, giving the first line to be written
    #  - push the next line from the file that the popped line came from onto the heap
    #  - repeat until heap becomes empty
    # 
    # note that since the files may contain wildly varying numbers of lines,
    # some of them will be exhausted quickly. this means the heap size should
    # steadily shrink over time, as it becomes impossible to replace an
    # extracted line with a new one from the source file
    lines_read = 0
    lines_written = 0
    start_time = time.time()

    # start by filling the heap
    for f in sorted_files.values():
        line = f.read_line()
        heapq.heappush(heap, (line.split(",")[0], line, f))
        lines_read += 1

    while len(sorted_files) > 0:
        # pop next line to be written
        next_id, next_line, src = heapq.heappop(heap)
        output_file.write(next_line)
        lines_written += 1

        # try to replace the popped line with one from the same file
        new_line = src.read_line()
        if new_line is not None:
            heapq.heappush(heap, (new_line.split(",")[0], new_line, src))
            lines_read += 1
        else:
            # ignore this file from now on
            print(f'Exhausted file {src}')
            sorted_files[src.name].close()
            del sorted_files[src.name]

        # update progress every 1 million lines written
        if lines_written % 1_000_000 == 0:
            print(f'> Written = {lines_written:,}, heap length={len(heap)}, files left={len(sorted_files)}')
            elapsed = time.time() - start_time
            total = 10_000_000_001 # total number of ClueWeb22_L records
            percent = 100 * (lines_written / total)
            remaining = (total - lines_written) / (lines_written / elapsed)
            print(f'> Completed: {percent:.3f}%, ETC = {fmt_timespan(remaining)}')

    output_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='Path to folder of already-sorted input files', required=True, type=str)
    parser.add_argument('-o', '--output', help='Filename of the final merged and sorted output file', required=True, type=str)
    args = parser.parse_args()
    run(args)
