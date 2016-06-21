#!/usr/bin/env python3
from subprocess import call
from sys import argv
from sys import stderr


def combine_files(file_list, output="all_reads.fq.gz"):
    """
    Add either fastq (.fq) or fastq gzip (.fq.gz) files to a file using gzip
    and cat in subprocess. By default,
    """
    if not output.endswith(".fq.gz"):
        output += ".fq.gz"

    for filename in file_list:
        if filename.endswith(".fq"):  # If ends in fq, compress and add it
            call("gzip -c {} >> {}".format(filename, output), shell=True)
        elif filename.endswith(".fq.gz"):  # If ends in fq.gz, just add it
            call("cat {} >> {}".format(filename, output), shell=True)
        else:
            print("Not adding: {}, files should be fq or fq.gz".format(
                filename), file=stderr)


if __name__ == "__main__":
    if len(argv) == 3:
        combine_files(argv[1], argv[2])
    elif len(argv) == 2:
        combine_files(argv[1], "all_reads.fq.gz")
    else:
        raise (RuntimeError("Unexpected Argument Count"))
