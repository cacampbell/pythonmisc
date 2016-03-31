#!/usr/bin/env python3
import os
import sys
from collections import namedtuple
from fabric.api import env
from fabric.api import run
from fabric.api import settings
from fabric.api import hide
from re import search
from subprocess import call
from multiprocessing import cpu_count
from multiprocessing import Pool

CPUs = cpu_count()
env.abort_on_prompts = True
env.colorize_errors = True
env.use_ssh_config = True

__USAGE_STATEMENT = """
    transfer.py

    DESCRIPTION
    Transfers entire file trees from source to destination. The source
    or destination can be on different machines or the same machine, so
    long as each host is accessible by SSH from the machine running
    this command

    USAGE

    python transfer.py [host1]:[source] [host1]:[destination] [threads]
    python transfer.py [host1]:[source] [host2]:[destination] [threads]
    python transfer.py [host1]:[source] [destination] [threads]
    python transfer.py [source] [host1]:[destination] [threads]
    python transfer.py [source] [destination] [threads]

    WHERE

    [host1] is a remote host
    [host2] is a remote host that may or may not be the same as host1
    [source] is the absolute path to the source directory
    [destination] is the absolute path to the destination directory
    [threads] is the number of parallel rsync threads to run
"""


def remote_to_remote(src_data, dest_data, threads):
    # First, attempt to connect directly from src to dest, using the user
    # ssh configuration on the src machine to connect. If that fails, then
    # Second, attempt to connect from the dest to the src, using the user
    # ssh configuration on the dest machine to connect. If that fails, then
    # Third, use the current machine as an intermediary bridge to connect
    # If this fails, then exit with error
    raise(RuntimeError("TODO: Not yet implemented"))


def get_files(src_data):
    env.host_string = src_data.Host
    command = ("find {} -type f | sed -n 's|^{}/||p'".format(src_data.Path,
                                                             src_data.Path))

    with settings(hide('running', 'commands')):
        background = run("").stdout.splitlines()
        background += ['\s+']
        output = run(command).stdout.splitlines()
        desired_output = []

        for line in output:
            for unwanted_line in background:
                if not search(unwanted_line.strip(), line):
                    if line != '':
                        desired_output += [line]

        return desired_output


def make_directories(dest_data, directories):
    env.host_string = dest_data.Host

    with settings(hide('running', 'commands')):
        for directory in directories:
            run("mkdir -p {}".format(os.path.join(dest_data.Path, directory)))


def commands(src_data, dest_data, files):
    c = ("rsync -avz {src_b}{{src_f}} {dest_b}{{dest_f}}").format(
        src_b=src_data.rsync_base, dest_b=dest_data.rsync_base)
    return [c.format(src_f=f, dest_f=f) for f in files]


def safe_call(command):
    try:
        call(command, shell=True)
    except Exception as e:
        print("Error occurred while executing:\n{}\n{}".format(command, e))


def dispatch(commands, threads):
    pool = Pool(processes=threads)
    pool.map(safe_call, commands)


def transfer(src_data, dest_data, threads):
    files = get_files(src_data)  # get files on host
    directories = set([os.path.dirname(x) for x in files])
    make_directories(dest_data, directories)
    cmds = commands(src_data, dest_data, files)
    dispatch(cmds, threads)


def parse_hostname_path(hostname_path):
    Data = namedtuple('Data', ['Host', 'Path', 'rsync_base'])

    if ":" in hostname_path:
        return Data(Host=hostname_path.split(":")[0],
                    Path=hostname_path.split(":")[1].rstrip(os.path.sep),
                    rsync_base=hostname_path.rstrip(os.path.sep) + os.path.sep)
    else:
        return Data(Host="localhost",
                    Path=os.path.abspath(hostname_path).rstrip(os.path.sep),
                    rsync_base=hostname_path.rstrip(os.path.sep) + os.path.sep)


def main(source, destination, threads):
    src_data = parse_hostname_path(source)
    dest_data = parse_hostname_path(destination)

    if src_data.Host != "localhost" and dest_data.Host != "localhost":
        if src_data.Host != dest_data.Host:
            # Neither is localhost, but have different names
            return remote_to_remote(src_data, dest_data, threads)

    # One is localhost, or transfering from remote server to itself
    return transfer(src_data, dest_data, threads)


if __name__ == "__main__":
    if len(sys.argv) == 4:
        # Use threadcount
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 3:
        # No threadcount specified, use autodetected threads
        main(sys.argv[1], sys.argv[2], CPUs)
    else:
        # Usage
        print(__USAGE_STATEMENT)
