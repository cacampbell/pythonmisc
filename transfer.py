#!/usr/bin/env python3
from collections import namedtuple
from fabric.api import env
from fabric.api import run
from fabric.api import settings
from fabric.api import hide
from fabric.exceptions import NetworkError
from os import path
from re import search
from sys import argv
from multiprocessing import cpu_count
from multiprocessing import Pool


class FabricAbortException(Exception):
    def __init__(self, message):
        super(Exception, self).__init__(message)


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

__retry_help__ = """
    All methods of transfer have failed. In order, this script connects:

        (1) Directly from the first remote to the second remote
        (2) Directly from the second remote to the first remote
        (3) By tunneling from remote 1 --> localhost --> remote2
        (4) By tunneling from remote 2 --> localhost --> remote1

    If these methods all failed, but you believe that you should be able to
    connect in one of these ways, it may help to attempt to connect manually
    with ssh first. This will allow addition of hosts, unlocking of keys, and
    additional information requisite for debugging connections between hosts.
    Once a connection has been successfully established with one of the above
    methods, retry this script again.

    Manual Commands:
        (1) localhost$ ssh -v -v -v [remote1]
            [remote1]$ ssh -v -v -v [remote2]
        (2) localhost$ ssh -v -v -v [remote2]
            [remote2]$ ssh -v -v -v [remote1]
        (3) localhost$ ssh -v -v -v -R localhost:[port]:[remote1]:22 [remote2]
            [remote2]$ ssh -v -v -v -p [port] localhost
        (4) localhost$ ssh -v -v -v -R localhost:[port]:[remote2]:22 [remote1]
            [remote1}$ ssh -v -v -v -p [port] localhost
"""

serverinfo = namedtuple('serverinfo', ['Host', 'Path', 'rsync_base'])


def safe_call(command):
    try:
        run(command)
    except FabricAbortException as err:
        print("Failed: {}".format(err))
    except (NetworkError, EOFError) as err:
        print("Failed: {}".format(err))
        print("Remote server issued hard disconnect...")
        raise(err)


def fab_parallel_2h(src, dest, cmds):
    extended_commands = ("ssh -N -R localhost:{p}:{s_h}:22 {d_h} "
                         "\"{{cmd}}\"").format(s_h=src.Host, d_h=dest.Host)
    commands = [extended_commands.format(cmd=c) for c in cmds]
    pool = Pool(processes=env.CPUs)
    pool.map(safe_call, commands)


def gnu_parallel_2h(src, dest, cmds):
    cmds_str = '\n'.join(cmds)
    safe_call(("ssh -N -R localhost:{p}:{s_h}:22 {d_h} "
               "\"echo -e '{cmds_str}' "
               "| parallel --gnu -j {jobs} {{}}\"").format(p=env.bridge_port,
                                                           s_h=src.Host,
                                                           d_h=dest.Host,
                                                           cmds_str=cmds_str,
                                                           jobs=env.CPUs))


def fab_parallel_1h(cmds):
    pool = Pool(processes=env.CPUs)
    pool.map(safe_call, cmds)


def gnu_parallel_1h(cmds):
    cmds_str = '\n'.join(cmds)
    safe_call("echo -e '{}' | parallel --gnu -j {} {{}}".format(cmds_str,
                                                                env.CPUs))


def intermediate_commands(src, dest, files):
    c = ("rsync -e 'ssh -p {p}' -avz {s} localhost:{d}")
    return [c.format(s=path.join(src.Path, f), d=path.join(dest.Path, f),
                     p=env.bridge_port) for f in files]


def intermediate_transfer(src, dest, files):
    env.host = 'localhost'

    try:
        print("Trying {} --> localhost --> {}...".format(src.Host, dest.Host))
        commands = intermediate_commands(src, dest, files)
        gnu_parallel_2h(commands)
    except (NetworkError, FabricAbortException, EOFError) as err:
        print("Failed: {}".format(err))
        try:
            print("Trying {} --> localhost --> {}...".format(dest.Host,
                                                             src.Host))
            commands = intermediate_commands(dest, src, files)
            direct_transfer(commands, dest.Host)
        except (NetworkError, FabricAbortException, EOFError) as err:
            print("Failed: {}".format(err))
            raise(err)


def direct_transfer(cmds, host):
    env.host_string = host

    try:
        print("Trying gnu parallel...")
        gnu_parallel_1h(cmds)
    except (NetworkError, FabricAbortException, EOFError) as err:
        print("Failed: {}".format(err))
        try:
            print("Trying parallel fabric connections...")
            fab_parallel_1h(cmds)
        except (NetworkError, FabricAbortException, EOFError) as err:
            print("Failed: {}".format(err))
            raise(err)


def format_commands(src, dest, files):
    c = ("rsync -avz {src_b}{{src_f}} {dest_b}{{dest_f}}").format(
        src_b=src.rsync_base, dest_b=dest.rsync_base)
    return [c.format(src_f=f, dest_f=f) for f in files]


def one_remote(src, dest, files):
    cmds = format_commands(src, dest, files)
    direct_transfer(cmds, "localhost")


def convert_to_localhost(data):
    local_data = serverinfo(Host="localhost",
                            Path=data.Path,
                            rsync_base=data.Path + path.sep)
    return local_data


def two_remote(src, dest, files):
    try:
        print("Trying forward transfer...")
        modified_src = convert_to_localhost(src)
        commands = format_commands(modified_src, dest, files)
        direct_transfer(commands, src.Host)
    except (NetworkError, FabricAbortException, EOFError) as err:
        print("Failed: {}".format(err))
        try:
            print("Trying reverse transfer...")
            modified_dest = convert_to_localhost(dest)
            commands = format_commands(src, modified_dest, files)
            direct_transfer(commands, dest.Host)
        except (NetworkError, FabricAbortException, EOFError) as err:
            print("Failed: {}".format(err))
            try:
                print("Trying intermediate transfer...")
                intermediate_transfer(src, dest, files)
            except (NetworkError, FabricAbortException, EOFError) as err:
                print("Failed: {}".format(err))
                print(__retry_help__)
                raise(err)


def make_directories(dest, directories):
    env.host_string = dest.Host

    for directory in directories:
        run("mkdir -p {}".format(path.join(dest.Path, directory)))


def get_files(src):
    env.host_string = src.Host
    command = ("find {}/ -type f | sed -n 's|^{}/||p'".format(src.Path,
                                                              src.Path))
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


def parse_hostname_path(hostname_path):
    if ":" in hostname_path:
        return serverinfo(Host=hostname_path.split(":")[0],
                          Path=hostname_path.split(":")[1].rstrip(path.sep),
                          rsync_base=hostname_path.rstrip(path.sep) + path.sep)
    else:
        return serverinfo(Host="localhost",
                          Path=path.abspath(hostname_path).rstrip(path.sep),
                          rsync_base=hostname_path.rstrip(path.sep) + path.sep)


def main(source, destination, threads=cpu_count()):
    env.CPUs = threads
    env.colorize_errors = True
    env.use_ssh_config = True
    env.abort_on_prompts = True
    env.abort_exception = FabricAbortException
    env.bridge_port = '5555'

    with settings(hide('commands')):
        src = parse_hostname_path(source)
        dest = parse_hostname_path(destination)
        files = get_files(src)  # get files on host
        directories = set([path.dirname(x) for x in files])
        make_directories(dest, directories)  # make directories on destination
        if src.Host != "localhost" and dest.Host != "localhost":
            if src.Host != dest.Host:
                # Neither is localhost, but have different names
                return two_remote(src, dest, files)

        # One is localhost, or transfering from remote server to itself
        return one_remote(src, dest, files)


if __name__ == "__main__":
    if len(argv) == 4:
        # Use threadcount
        main(argv[1], argv[2], argv[3])
    elif len(argv) == 3:
        # No threadcount specified, use autodetected threads
        main(argv[1], argv[2])
    else:
        # Usage
        print(__USAGE_STATEMENT)
