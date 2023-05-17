#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

if [ $# -ne 5 ]
then 
    echo "Missing arguments: <input CSV file> <output path> <output file prefix> <number of lines per chunk> <cores to use>"
    exit 0
fi

CSV=$(realpath "${1}")
OUT_PATH="${2}"
PREFIX="${3}"
LINES="${4}"
CORES="${5}"

GZIP=gzip
# use pigz (https://github.com/madler/pigz/) if possible
# to parallelize the compression process
if ! [ -x "$(command -v pigz)" ]
then
    echo "> pigz not found, using gzip"
else
    echo "> using pigz for compression!"
    GZIP=pigz
fi

echo "> Splitting ${CSV} into chunks of ${LINES} in ${OUT_PATH}"

mkdir -p "${OUT_PATH}"
pushd "${OUT_PATH}"

# -d means use numeric suffixes starting from 0
# -l is the number of lines per chunk
# --additional-suffix is used to append '.csv' to each new filename
# the prefix is supplied as the last argument
split -d -l "${LINES}" --additional-suffix=.csv "${CSV}" "${PREFIX}"

echo "> Finished splitting files"

# now compress and checksum each of the split files
for f in "${PREFIX}"*.csv
do
    echo "> Compressing ${f}"
    # -9 means best compression level
    # for pigz, -p sets the number of cores to use
    if [ "${GZIP}" == "pigz" ]
    then
        pigz -p "${CORES}" -9 -v "${f}"
    else
        gzip -9 -v "${f}"
    fi
    echo "> Checksumming ${f}"
    md5sum "${f}.gz" | awk '{ print $1; }' > "${f}.gz.checksum"
done

popd "${OUT_PATH}"
