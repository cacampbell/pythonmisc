#!/usr/bin/env python
import os
import sys
from multiprocessing.dummy import Pool
from subprocess import call
from functools import partial


def get_files(root):
    """
    Fills a list with every gz file below the root directory
    """
    filelist = []

    for root, directories, files in os.walk(root):
        for filename in files:
            absolute_path = os.path.abspath(os.path.join(root, filename))
            filelist.append(absolute_path)

    return filelist


def make_commands(command_and_flags, files):
    """
    Formats commands for unzipping for each file
    """
    cmds = []

    for filename in files:
        command = command_and_flags + filename
        cmds.append(command)

    return cmds


def run_commands(cmds, max_processes):
    """
    Runs commands max_processes at a time provided in the command list (cmds)
    """
    pool = Pool(max_processes)
    for i, returncode in enumerate(pool.imap(partial(call, shell=True), cmds)):
        if returncode != 0:
            print ("%d command failed: %d" % (i, returncode))


def main():
    """
    unzips all gz files below the current directory
    """
    command_and_flags = sys.argv[1]
    files = get_files(os.path.curdir)
    cmds = make_commands(command_and_flags, files)
    run_commands(cmds, 8)


if __name__ == "__main__":
    main()
