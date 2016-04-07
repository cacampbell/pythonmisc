#!/usr/bin/env python3
from collections import namedtuple
from fabric.api import env
from fabric.api import run
from fabric.api import settings
from fabric.exceptions import NetworkError
from fabric.api import hide
from os import path
import paramiko
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

        (1) Directly from {src} to {dest}
        (2) Directly from {dest} to {src}
        (3) By tunneling from {src} --> localhost --> {dest}
        (4) By tunneling from {dest} --> localhost --> {src}

    If these methods all failed, but you believe that you should be able to
    connect in one of these ways, it may help to attempt to connect manually
    with ssh first. This will allow addition of hosts, unlocking of keys, and
    will provide information requisite for debugging connections between hosts.
    Once a connection has been successfully established with one of the above
    methods, retry this script again.

    Manual Commands:
        (1) localhost$ ssh -v -v -v {src}
            {src}$ ssh -v -v -v {dest}
        (2) localhost$ ssh -v -v -v {dest}
            {dest}$ ssh -v -v -v {src}
        (3) localhost$ ssh -v -v -v -R localhost:{port}:{src}:22 {dest}
            {dest}$ ssh -v -v -v -p {port} localhost
        (4) localhost$ ssh -v -v -v -R localhost:{port}:{dest}:22 {src}
            {src}$ ssh -v -v -v -p {port} localhost
"""

serverinfo = namedtuple('serverinfo', ['host', 'path', 'rsync_base'])


def safe_call(command):
    try:
        # run(command)
        print(command)
    except FabricAbortException as err:
        print("Failed: {}".format(err))
    except (NetworkError, EOFError) as err:
        print("Failed: {}".format(err))
        print("Remote server issued hard disconnect...")
        raise(err)
    finally:
        with settings(warn_only=True):
            m = search(":/.*/\s*?", command)
            run(("kill -9 `ps aux | grep [rsync|parallel] | {} | "
                 "awk '{print $2}'`").format(m.group(1)))


def e_quotes(string):
    return string.replace('"', '\\"').replace("'", "\\'")


def fab_parallel_2h(src, dest, cmds):
    extended_commands = ("ssh -o \"NumberOfPasswordPrompts 0\" "
                         "-R localhost:{p}:{s_h}:22 {d_h} "
                         "\"{{cmd}}\"").format(s_h=src.host, d_h=dest.host)
    commands = [extended_commands.format(cmd=c) for c in cmds]
    pool = Pool(processes=env.CPUs)
    pool.map(safe_call, commands)


def gnu_parallel_2h(src, dest, cmds):
    cmds_str = '\n'.join(cmds)
    safe_call(("ssh -o \"NumberOfPasswordPrompts 0\" "
               "-R localhost:{p}:{s_h}:22 {d_h} \"echo -e '{c}' "
               "| parallel --gnu -j {jobs} {{}}\"").format(p=env.bridge_port,
                                                           s_h=src.host,
                                                           d_h=dest.host,
                                                           c=cmds_str,
                                                           jobs=env.CPUs))


def fab_parallel_1h(cmds):
    pool = Pool(processes=env.CPUs)
    pool.map(safe_call, cmds)


def gnu_parallel_1h(cmds):
    cmds_str = '\n'.join(cmds)
    safe_call("echo -e '{}' | parallel --gnu -j {} {{}}".format(cmds_str,
                                                                env.CPUs))


def intermediate_commands(src, dest, files):
    c = ("rsync -e 'ssh -o \"NumberOfPasswordPrompts 0\" -p {p}' "
         "-avz {s} localhost:{d}")
    return [e_quotes(c).format(s=path.join(src.path, f),
                               d=path.join(dest.path, f),
                               p=env.bridge_port) for f in files]


def intermediate_transfer(src, dest, files):
    env.host = 'localhost'
    print("Trying {} --> localhost --> {}...".format(src.host, dest.host))

    try:
        print("Trying gnu parallel...")
        commands = intermediate_commands(src, dest, files)
        gnu_parallel_2h(src, dest, commands)
    except Exception as err:
        print("Failed: {}".format(err))
        try:
            print("Trying parallel fabric connections...")
            commands = intermediate_commands(src, dest, files)
            fab_parallel_2h(src, dest, commands)
        except Exception as err:
            print("Failed: {}".format(err))
            raise(err)


def direct_transfer(cmds, host):
    env.host_string = host

    try:
        print("Trying gnu parallel...")
        gnu_parallel_1h(cmds)
    except Exception as err:
        print("Failed: {}".format(err))
        try:
            print("Trying parallel fabric connections...")
            fab_parallel_1h(cmds)
        except Exception as err:
            print("Failed: {}".format(err))
            raise(err)


def format_commands(src, dest, files):
    c = ("rsync -e 'ssh -o \"NumberOfPasswordPrompts 0\"' "
         "-avz {src_b}{{src_f}} {dest_b}{{dest_f}}").format(
        src_b=src.rsync_base, dest_b=dest.rsync_base)
    return [e_quotes(c).format(src_f=f, dest_f=f) for f in files]


def one_remote(src, dest, files):
    cmds = format_commands(src, dest, files)
    direct_transfer(cmds, "localhost")


def to_localhost(data):
    local_data = serverinfo(host="localhost",
                            path=data.path,
                            rsync_base=data.path)
    return local_data


def two_remote(src, dest, files):
    try:
        print("Trying forward transfer...")
        modified_src = to_localhost(src)
        commands = format_commands(modified_src, dest, files)
        direct_transfer(commands, src.host)
    except Exception as err:
        print("Failed: {}".format(err))
        try:
            print("Trying reverse transfer...")
            modified_dest = to_localhost(dest)
            commands = format_commands(src, modified_dest, files)
            direct_transfer(commands, dest.host)
        except Exception as err:
            print("Failed: {}".format(err))
            try:
                print("Trying intermediate transfer...")
                intermediate_transfer(src, dest, files)
            except Exception as err:
                print("Failed: {}".format(err))
                try:
                    intermediate_transfer(dest, src, files)
                except Exception as err:
                    print(__retry_help__.format(src=src.host,
                                                dest=dest.host,
                                                port=env.bridge_port))
                    raise(err)


def make_directories(dest, directories):
    env.host_string = dest.host

    for directory in directories:
        run("mkdir -p {}".format(path.join(dest.path, directory)))


def get_files(src):
    env.host_string = src.host
    command = ("find {}/ -type f | sed -n 's|^{}/||p'".format(src.path,
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
                          path=host_parts[1] + path.sep,
                          rsync_base=hostname + path.sep)
    else:
        return serverinfo(host="localhost",
                          path=path.abspath(hostname) + path.sep,
                          rsync_base=hostname + path.sep)


def set_up(src, dest, threads):
    env.CPUs = threads
    env.colorize_errors = True
    env.use_ssh_config = True
    env.abort_on_prompts = True
    env.abort_exception = FabricAbortException
    env.bridge_port = '5555'
    paramiko.util.log_to_file("/dev/null")
    files = get_files(src)  # get files on host
    directories = set([path.dirname(x) for x in files])
    make_directories(dest, directories)  # make directories on destination
    return files


def main(source, destination, threads=cpu_count()):
    src = parse_hostname_path(source)
    dest = parse_hostname_path(destination)
    files = set_up(src, dest, threads)
    with settings(hide('everything')):
        if src.host != "localhost" and dest.host != "localhost":
            if src.host != dest.host:
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
