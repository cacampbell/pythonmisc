#!/usr/bin/env python3
from sys import argv
from os import walk
from os.path import basename
from os.path import join
from Bash import bash


def get_barcode(filename):
    display_filename = "cat {}".format(filename)

    if filename.endswith(".gz"):
        display_filename = "gunzip -c {}".format(filename)

    command = ("{} | head -n 10000 | grep ^@ | cut -d':' -f10 | tr -d ' ' "
               "| sort | uniq -c | sort -nr | head -1 | sed -e "
               "'s/^[[:space:]]*//' | cut -d ' ' -f2").format(display_filename)
    return bash(command)[0]


def main(root, output):
    with open(output, "w") as fh:
        for root, directories, files in walk(root):
            for filename in files:
                if filename.endswith(".fq.gz") \
                        or filename.endswith(".fastq.gz") \
                        or filename.endswith(".fq") \
                        or filename.endswith(".fastq"):
                    if "_R1" in filename:
                        abs_path = join(root, filename)
                        base = basename(abs_path)
                        name = base.split(".")[0].replace("_R1", "_pe")
                        barcode = get_barcode(abs_path)
                        fh.write("{} {}".format(name, barcode))


if __name__ == "__main__":
    if len(argv) == 3:
        main(argv[1], argv[2])
    else:
        print("generate_barcode_file.py <root directory> <output>")
