#!/usr/bin/env python
import os
from multiprocessing import Pool
import subprocess
from functools import partial


def get_files(directory):
    file_list = []

    for root, directories, files in os.walk(directory):
        for filename in files:
            absolute_path = os.path.join(root, filename)
            file_list.append(absolute_path)

    return file_list


def call_rsync(file_list, destination, threads):
    def make_command(filename):
        return "rsync -arvz %s %s --append-verify" % (filename, destination)

    subprocess_partial = partial(subprocess.call, shell=True)
    commands = [make_command(f) for f in file_list]

    with Pool(processes=threads) as pool:
        pool.map(subprocess_partial, commands, 1)


def main():
    files = get_files("/home/cacampbe/pear_bwa")
    call_rsync(files, "/share/nealedata2", 30)


if __name__ == "__main__":
    main()
