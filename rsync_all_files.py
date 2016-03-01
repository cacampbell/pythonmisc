#!/usr/bin/env python
from __future__ import print_function
import os
from multiprocessing import Pool
import subprocess
from functools import partial
import re
import errno

max_processes = 20
input_root = "/group/nealedata5/Psme_reseq/"
output_root = "/group/nealedata4/Psme_reseq/"


def get_files(d):
    filenames = []

    for root, _, files in os.walk(d):
        for filename in files:
            filenames += [os.path.join(root[len(output_root):], filename)]

    return filenames


def make_directories(directories):
    for folder in directories:
        abs_folder = os.path.join(input_root, folder)
        try:
            os.makedirs(abs_folder)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(abs_folder):
                pass


def rsync_commands(files):
    cmds = []

    for filename in files:
        inputf = os.path.join(input_root, filename)
        outputf = os.path.join(output_root, filename)
        outdir = os.path.dirname(outputf)
        cmd = ('rsync -avzP {src} {dest}').format(src=inputf, dest=outdir)
        cmd = re.sub("\r\n?|\n", '', cmd)
        cmds += [cmd]

    return cmds


def dispatch(commands):
    pool = Pool(max_processes)
    for i, returncode in enumerate(pool.imap(partial(subprocess.call,
                                                     shell=True), commands)):
        if returncode != 0:
            print("{} command failed: {}".format(i, returncode))


def main():
    files = get_files(input_root)
    print(files)
    directories = set(
        [os.path.join(output_root, os.path.dirname(x)) for x in files])
    make_directories(directories)
    commands = rsync_commands(files)
    dispatch(commands)

if __name__ == "__main__":
    main()
