#!/usr/bin/env python3
from Bash import bash
from parallel_command_parse import run_with_args
from os.path import basename


def main(fastq, mapped):
    (fastq_sizes, err1) = bash("du -sb {}/*.fq.gz".format(fastq.rstrip('/')))
    (map_sizes, err2) = bash("du -sb {}/*001.sam".format(mapped.rstrip('/')))
    read1 = {}
    read2 = {}
    mapped = {}

    for line in fastq_sizes.splitlines():
        chunks = line.split()
        name = basename(chunks[1]).split("_R")[0]
        size = chunks[0]
        if "_R1" in chunks[1]:
            read1[name] = size
        if "_R2" in chunks[1]:
            read2[name] = size

    for line in map_sizes.splitlines():
        chunks = line.split()
        name = basename(chunks[1]).split("_pe")[0]
        size = chunks[0]
        mapped[name] = size

    table = {}
    for key, val in mapped.items():
        table[key] = [read1[key], read2[key], val]

    with open("filesizes", "w") as fh:
        for key, val in table.items():
            fh.write("{}\t{}\t{}\t{}\n".format(key, *val))

    (out, err) = bash("plot_map_sizes.R")

if __name__ == "__main__":
    run_with_args(main)
