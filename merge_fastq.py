#!/usr/bin/env python
from Bio import SeqIO
from FileHydra import FileHydra
import threading
import sys
import os


def merge_files(files, output_file, threads=4):
    lock = threading.Lock()
    with FileHydra(" ".join(files), "r+ " * len(files)) as queue:

        def read_sequences(file_handle):
            with lock, open(output_file, "w+") as output:
                SeqIO.write(SeqIO.read(file_handle, "fasta"), output, "fasta")

        def worker():
            file_handle = queue.get()
            read_sequences(file_handle)

        for i in range(threads):
            t = threading.Thread(target=worker)
            t.daemon = True
            t.start()


def get_files(input_root):
    """
    Read files below input_root into list
    """
    filenames = []

    for root, dirs, files in os.walk(input_root):
        for filename in files:
            abs_path = os.path.join(root, filename)
            if abs_path.endswith(".fa") or abs_path.endswith(".fasta"):
                filenames.extend([abs_path])

    return filenames


def main(input_root, output_file):
    """
    Gather fq files below the input_root and merge them into output_file
    :input_root: str: path to input root
    :output_file: str: path to output_file
    """
    files = get_files(input_root)
    merge_files(files, output_file)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
