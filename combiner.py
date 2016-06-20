#!/usr/bin/env python3
from sys import argv


def combine_files(file_list, filename="all_reads.fq.gz", format="fastq"):
    pass


if __name__ == "__main__":
    if len(argv) == 4:
        combine_files(argv[1], argv[2], argv[3])
    elif len(argv) == 3:
        combine_files(argv[1], argv[2], "fastq")
    elif len(argv) == 2:
        combine_files(argv[1], "all_reads.fq.gz", "fastq")
    else:
        raise (RuntimeError("Unexpected Argument Count"))
