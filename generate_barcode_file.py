#!/usr/bin/env python3
from sys import argv

from os import walk
from os.path import basename
from os.path import join

from Bash import bash


def get_barcode(filename):
    command = ("cat {} | head -n 10000 | grep ^@ | cut -d' ' -f10 | tr -d ' ' "
               "| sort | uniq -c | sort -nr | head -n 1 | cut -d' ' "
               "-f7").format(filename)
    return bash(command)


def main(root, output):
    for root, directories, files in walk(root):
        for filename in files:
            if filename.endswith(".fq") or filename.endswit(".fastq"):
                abs = join(root, filename)
                base = basename(abs)
                name = base.split(".")[0]
                barcode = get_barcode(filename)

                with open(output, "w") as fh:
                    fh.write("{} {}\n".format(name, barcode))


if __name__ == "__main__":
    if len(argv) == 3:
        main(argv[1], argv[2])
    else:
        print("generate_barcode_file.py <root directory> <output>")
