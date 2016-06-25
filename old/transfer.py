#!/usr/bin/env python3
from getpass import getuser
from multiprocessing import Pool
from multiprocessing import cpu_count
from subprocess import PIPE
from subprocess import Popen
from sys import argv
from sys import stderr as syserr

import paramiko
from fabric.api import env
from fabric.api import run
from fabric.exceptions import NetworkError
from os import linesep
from os import path
from re import search

# {{{ Usage Statements
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
    the rsync format_commands manually for some subset of the files to see if you are
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
        (2) By tunneling from {src} --> {proxy} --> {dest},
            running rsync on {src}
        (3) By tunneling from {dest} --> {proxy} --> {src},
            running rsync on {dest}

    If these methods all failed, but you believe that you should be able to
    connect in one of these ways, it may help to attempt to connect manually
    first. This will provide additional diagnostic information and will
    establish known_hosts, ssh identities, etc. Once you are able to
    successfully connect with ssh, try this script again.

    Manual Commands:
        (2) localhost$ ssh -v -v -v -R {proxy}:{port}:{dest}:22 {src}
            {src}$ ssh -v -v -v -p {port} {proxy}
        (3) localhost$ ssh -v -v -v -R localhost:{port}:{src}:22 {dest}
            {dest}$ ssh -v -v -v -p {port} {proxy}
"""
# }}} Usage Statements


class serverinfo:
    def __init__(self, host, path):
        self.host = host
        self.path = path

    def to_local(self):
        self.host = "localhost"


class FabricAbort(Exception):
    def __init__(self, message):
        super(FabricAbort, self).__init__(message)


def fab(command):
    if env.host_string != "localhost":
        return run(command).stdout.splitlines()
    else:
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        (out, err) = process.communicate()
        return(out.decode('utf-8').split(linesep))


def safe_call(command):
    try:
        fab(command)
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Command failed: {}".format(err))
        raise(err)


def multiprocess_run(commands):
    pool = Pool(int(env.CPUs))
    pool.map(safe_call, commands)


def parallel_run(command_str):
    parallel_command = ("echo \"{c}\" | parallel --gnu -j {t}")
    safe_call(parallel_command.format(c=command_str, t=env.CPUs))


def run_commands(commands):
    try:
        command_str = '\n'.join(commands)
        parallel_run(command_str)
    except (FabricAbort, NetworkError, OSError, EOFError):
        multiprocess_run(commands)


def remote_forward_transfer(src, dest, files):
    try:
        env.host_string = src.host
        src.to_local()
        single_remote_transfer(src, dest, files)
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Direct from {} to {} on {} failed: {}".format(
            src.host, dest.host, src.host, err), file=syserr)
        try:
            env.host_string = src.dest
            dest.to_local()
            single_remote_transfer(src, dest, files)
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print("Direct from {} to {} on {} failed: {}".format(
                src.host, dest.host, dest.host, err), file=syserr)
            raise(err)


def forward_proxy_commands(src, dest, files):
    cmd = ("rsync -avz -e 'ssh -p {p}' {w}@localhost:{src} {dest}")
    return [cmd.format(p=env.bridge_port, w=env.whoami,
                       src=path.join(src.path, f),
                       dest=path.join(dest.path, f)) for f in files]


def forward_proxy_transfer(src, dest, files):
    cmds = forward_proxy_commands(src, dest, files)
    wrap_command = [("ssh -R localhost:{p}:{src}:22 {dest} \"{cmd}\"").format(
        p=env.bridge_port, dest=dest.host, src=src.host, cmd=c) for c in cmds]
    run_commands(wrap_command)


def reverse_proxy_transfer(src, dest, files):
    cmds = reverse_proxy_transfer(src, dest, files)
    wrap_command = [("ssh -R localhost:{p}:{dest}:22 {src} \"{cmd}\"").format(
        p=env.bridge_port, dest=dest.host, host=src.host, cmd=c) for c in cmds]
    run_commands(wrap_command)


def remote_proxy_transfer(src, dest, files):
    env.host_string = "localhost"
    # TODO: Fabric Merge Request 1218 implements local port forwarding
    # for local port forwarding. Once this implementation has been relesed in
    # fabric, this section should be reimplemented
    try:
        forward_proxy_transfer(src, dest, files)
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print(("Transfer from {} --> localhost --> {}"
               " with rsync from {} failed: {}").format(
                   src.host, dest.host, src.host, err), file=syserr)
        try:
            reverse_proxy_transfer(src, dest, files)
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print(("Transfer from {} --> localhost --> {}"
                   " with rsync from {} failed: {}").format(
                       src.host, dest.host, src.host, err), file=syserr)
            raise(err)


def double_remote_transfer(src, dest, files):
    try:
        remote_forward_transfer(src, dest, files)
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Direct from {} to {} failed: {}".format(
            src.host, dest.host, err), file=syserr)
        try:
            remote_proxy_transfer(src, env.proxy_h, dest, files)
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print(__intermediate_help__.format(
                src=src.host, dest=dest.host, port=env.port, proxy=env.proxy_h),
                file=syserr)
            raise(err)


def single_forward_commands(src, dest, files):
    cmd = ("rsync -avz {}:{} {}:{}")
    return [cmd.format(src.host, path.join(src.path, f),
                       dest.host, path.join(dest.path, f)) for f in files]


def single_reverse_commands(src, dest, files):
    cmd = ("ssh -R localhost:{p}:{dest}:22 {src} \"rsync -avz -e 'ssh -p {p}' "
           "{whoami}@localhost:{src_p} {dest_p}\"")
    return [cmd.format(p=env.bridge_port, dest=dest.host, src=src.path,
                       whoami=env.whoami, src_p=path.join(src.path, f),
                       dest_p=path.join(dest.path, f)) for f in files]


def single_remote_transfer(src, dest, files):
    try:
        commands = single_forward_commands(src, dest, files)
        run_commands(commands)
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Direct transfer from {} to {} failed".format(src.host,
                                                            dest.host))
        try:
            commands = single_reverse_commands(src, dest, files)
            run_commands(commands)
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print(__direct_help__.format(
                src=src.host, dest=dest.host, port=env.bridge_port,
                whoami=env.whoami))
            raise(err)


def local_commands(src, dest, files):
    c_str = ("rsync -avz {} {}")
    return [c_str.format(path.join(src.path, f),
                         path.join(dest.path, f)) for f in files]


def local_transfer(src, dest, files):
    try:
        commands = local_commands(src, dest, files)
        run_commands(commands)
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print(__local_help__.format(src.path, dest.path, src.host))
        raise(err)


def make_directories(dest, directories):
    env.host_string = dest.host
    [fab("mkdir -p {}".format(path.join(dest.path, d))) for d in directories]


def get_files(src):
    env.host_string = src.host
    command = ("find {} -type f | sed -n 's|^{}/||p'").format(src.path,
                                                              src.path)
    background = fab("")
    background += ['\s+']
    out = fab(command)
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
        env.proxy_h = "localhost"
        env.colorize_errors = True
        env.use_ssh_config = True
        env.abort_on_prompts = True
        env.abort_exception = FabricAbort
        env.bridge_port = '4235'  # Some large port that isn't used by anything
        paramiko.util.log_to_file("/dev/null")  # squelch warnings about log
        files = get_files(src)  # get files on host
        directories = set([path.dirname(x) for x in files])
        make_directories(dest, directories)  # make directories on destination
        env.host_string = "localhost"  # localhost runs most format_commands
        return(files)  # hand off file list
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Error during setup: {}".format(err))
        raise(err)


def main(source, destination, threads=cpu_count()):
    src = parse_hostname_path(source)
    dest = parse_hostname_path(destination)
    files = set_up(src, dest, threads)

    try:
        if src.host == "localhost" or dest.host == "localhost":
            if src.host == "localhost" and dest.host == "localhost":
                local_transfer(src, dest, files)
            else:
                single_remote_transfer(src, dest, files)
        else:
            double_remote_transfer(src, dest, files)
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print('All methods of transfer failed...')
        raise(err)


if __name__ == "__main__":
    if len(argv) == 4:
        main(argv[1], argv[2], argv[3])
    elif len(argv) == 3:
        main(argv[1], argv[2])
    else:
        print(__USAGE_STATEMENT)
