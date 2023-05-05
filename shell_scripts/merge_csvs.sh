#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

# This script takes a set of *already sorted* CSV files with .csv.sorted extensions,
# and runs 'sort' in merge mode on them. In this mode, the tool assumes the input files
# are already sorted and will merge the contents into a single fully-sorted output file.
# 
# This might be useful for sorting files based on ClueWeb-ID when the filesizes can be
# very large. 

# Ignore locale since ClueWeb-ID fields will only contain ASCII characters (otherwise
# should use locale-dependent sorting to correctly handle multi-byte Unicode sequences etc)
export LC_ALL=C

if [ $# -ne 4 ]
then
    echo "Usage: merge_sort.sh <path to source files> <path to output file> <# of parallel workers for sort> <memory buffer size in GB for sort>"
    exit 0
fi

SRC="${1}"
DST="${2}"
PARALLEL="${3}"
BUFFERSZ="${4}"

echo "> Merging started $(date)"
sort -t, -k1 --parallel="${PARALLEL}" -S "${BUFFERSZ}"G -m "${SRC}"/*.csv.sorted > "${DST}"
echo "> Merging completed! $(date)"

