#!/usr/bin/env python3
from collections import namedtuple
from fabric.api import env
from fabric.api import run
from fabric.api import settings
from fabric.exceptions import NetworkError
from fabric.api import hide
from getpass import getuser
from os import path
import paramiko
from re import search
from sys import argv
from multiprocessing import cpu_count
from multiprocessing import Pool


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
__local_help__ = """
    It appears that the local transfer has failed. This is sexecuted by running
    rsync -avz {src_path} {dest_path} on {server}.

    If you believe that you should be able to make this transfer, try running
    the rsync commands manually for some subset of the files to see if you are
    able.
"""


__direct_help__ = """
    All methods of direct transfer have failed. In order, this script
    connects:

        (1) From {src} to {dest}, running rsync on {src}
        (2) From {src} to {dest}, using reverse tunnel, running rsync on {dest}
        (3) From {dest} to {src}, running rsync on {dest}
        (4) From {dest} to {src}, using reverse tunnel, running rsync on {src}

    If these methods all failed, but you believe that you should be able to
    connect in one of these ways, it may help to attempt to connect manually
    first. This will provide additional diagnostic information and will
    establish known_hosts, ssh identities, etc. Once you are able to
    successfully connect with ssh, try this script again.

        (1) {src}$ ssh -v -v -v {dest}
        (2) {src}$ ssh -v -v -v -R {port}:localhost:22 {dest}
            {dest}$ ssh -v -v -v -p {port} {whoami}@localhost
        (3) {dest}$ ssh -v -v -v {src}
        (4) {dest}$ ssh -v -v -v -R {port}:localhost:22 {src}
            {dest}$ ssh -v -v -v -p {port} {whoami}@localhost

"""


__intermediate_help__ = """
    All methods of intermediate transfer have failed. In order,
    this script connects:

        (1) Directly [see direct help]
        (2) By tunneling from {src} --> localhost --> {dest},
            running rsync on {src}
        (3) By tunneling from {dest} --> localhost --> {src},
            running rsync on {dest}

    If these methods all failed, but you believe that you should be able to
    connect in one of these ways, it may help to attempt to connect manually
    first. This will provide additional diagnostic information and will
    establish known_hosts, ssh identities, etc. Once you are able to
    successfully connect with ssh, try this script again.

    Manual Commands:
        (2) localhost$ ssh -v -v -v -R localhost:{port}:{dest}:22 {src}
            {src}$ ssh -v -v -v -p {port} {whoami}@localhost
        (3) localhost$ ssh -v -v -v -R localhost:{port}:{src}:22 {dest}
            {dest}$ ssh -v -v -v -p {port} {whoami}@localhost
"""

serverinfo = namedtuple('serverinfo', ['host', 'path'])


class FabricAbortException(Exception):
    def __init__(self, message):
        super(FabricAbortException, self).__init__(message)


def safe_call(command):
    try:
        run(command)
    except (FabricAbortException, NetworkError, OSError):
        pass


def multiprocess_run(commands):
    pool = Pool(env.CPUs)
    pool.map(safe_call, commands)


def parallel_run(command_str):
    parallel_command = ("echo -e \"{c}\" | parallel --gnu -j {t}")
    run(parallel_command.format(c=command_str, t=env.CPUs))


def run_commands(commands):
    try:
        command_str = '\n'.join(commands)
        parallel_run(command_str)
    except (FabricAbortException, NetworkError, OSError):
        multiprocess_run(commands)


def double_remote_transfer():
    pass


def single_remote_transfer():
    pass


def local_commands(src, dest, files):
    c_str = ("rsync -avz {} {}")
    return [c_str.format(src.path, dest.path) for f in files]


def local_transfer(src, dest, files):
    try:
        env.host_string = src.host
        commands = local_commands(src, dest, files)
        run_commands(commands)
    except (FabricAbortException, NetworkError, OSError) as err:
        print(__local_help__.format(src.path, dest.path, src.host))
        raise(err)


def make_directories(dest, directories):
    env.host_string = dest.host

    for directory in directories:
        run("mkdir -p {}".format(path.join(dest.path, directory)))


def get_files(src):
    env.host_string = src.host
    command = ("find {} -type f | sed -n 's|^{}||p'".format(src.path,
                                                            src.path))
    background = run("").stdout.splitlines()
    background += ['\s+']
    out = run(command).stdout.splitlines()
    desired_output = []

    for line in out:
        for unwanted_line in background:
            if not search(unwanted_line.strip(), line):
                if line != '':
                    desired_output += [line]

    return desired_output


def parse_hostname_path(hostname_path):
    hostname = hostname_path.rstrip(path.sep)

    if ":" in hostname:
        host_parts = hostname.split(":")
        return serverinfo(host=host_parts[0],
                          path=host_parts[1] + path.sep)
    else:
        return serverinfo(host="localhost",
                          path=path.abspath(path.abspath(hostname) + path.sep))


def set_up(src, dest, threads):
    try:
        env.CPUs = threads
        env.whoami = getuser()
        env.colorize_errors = True
        env.use_ssh_config = True
        env.abort_on_prompts = True
        env.abort_exception = FabricAbortException
        env.bridge_port = '4245'  # Some large port that isn't used by anything
        paramiko.util.log_to_file("/dev/null")  # squelch warnings about log
        files = get_files(src)  # get files on host
        directories = set([path.dirname(x) for x in files])
        make_directories(dest, directories)  # make directories on destination
        return(files)  # hand off file list
    except (NetworkError, FabricAbortException, OSError) as err:
        print("Error during setup: {}".format(err))
        raise(err)


def main(source, destination, threads=cpu_count()):
    with settings(hide('commands', 'stdout', 'stderr')):
        src = parse_hostname_path(source)
        dest = parse_hostname_path(destination)
        files = set_up(src, dest, threads)
        try:
            if src.host != dest.host:
                if src.host == "localhost" or dest.host == "localhost":
                    single_remote_transfer(src, dest, files)
                else:
                    double_remote_transfer(src, dest, files)
            else:
                local_transfer(src, dest, files)
        except (FabricAbortException, NetworkError, OSError) as err:
            print('All methods of transfer failed...')
            raise(err)


if __name__ == "__main__":
    if len(argv) == 4:
        main(argv[1], argv[2], argv[3])
    elif len(argv) == 3:
        main(argv[1], argv[2])
    else:
        print(__USAGE_STATEMENT)
