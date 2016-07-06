#!/usr/bin/env python3
from sys import argv

from io import BufferedReader
from io import TextIOWrapper
from os import remove
from os.path import isfile

from Decompress import decompress


def combine_files(file_list, output="all_reads.fq"):
    """
    Add either fastq (.fq) or fastq gzip (.fq.gz) files to a file using gzip
    and cat in subprocess. By default,
    """
    if not output.endswith(".fq"):
        output += ".fq"

    if isfile(output):
        return

    def zipped(f):
        return (f.endswith(".gz") or f.endswith(".bz2") or f.endswith(".zip"))

    fq_files = [f for f in file_list if f.endswith(".fq")]
    fqz_files = [f for f in file_list if not zipped(f)]

    try:
        with open(output, 'w+') as o_h:
            for filename in fq_files:
                with open(filename, 'r+') as fh:
                    for line in fh:
                        o_h.write(line)
            for filename in fqz_files:  # java-like boilerplate for iteration
                with TextIOWrapper(BufferedReader(decompress(filename))) as gh:
                    for line in gh:
                        o_h.write(line)
    except (IOError, OSError) as err:
       remove(output)
       raise(err)


if __name__ == "__main__":
    if len(argv) == 3:
        combine_files(argv[1], argv[2])
    elif len(argv) == 2:
        combine_files(argv[1], "all_reads.fq.gz")
    else:
        raise (RuntimeError("Unexpected Argument Count"))