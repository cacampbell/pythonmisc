#!/usr/bin/env python3
from fabric.api import env
from fabric.api import run
from fabric.exceptions import NetworkError
from getpass import getuser
from os import path
from os import linesep
import paramiko
from sys import argv
from subprocess import Popen
from subprocess import PIPE


# {{{ Usage Statements
__USAGE_STATEMENT = """
    transfer.py

    DESCRIPTION
    Transfers entire file trees from source to destination. The source
    or destination can be on different machines or the same machine, so
    long as each host is accessible by SSH from the machine running
    this command

    USAGE

    python transfer.py [host1]:[source] [host1]:[destination]
    python transfer.py [host1]:[source] [host2]:[destination]
    python transfer.py [host1]:[source] [destination]
    python transfer.py [source] [host1]:[destination]
    python transfer.py [source] [destination]

    WHERE

    [host1] is a remote host
    [host2] is a remote host that may or may not be the same as host1
    [source] is the absolute path to the source directory
    [destination] is the absolute path to the destination directory
"""


class serverinfo:
    def __init__(self, host, path):
        self.host = host
        self.path = path

    def to_local(self):
        self.host = "localhost"

    def target(self):
        if self.host == "localhost":
            return(self.path)
        else:
            return("{}:{}".format(self.host, self.path))


class FabricAbort(Exception):
    def __init__(self, message):
        super(FabricAbort, self).__init__(message)


def fab(command):
    if env.host_string != "localhost":
        return run(command).stdout.splitlines()
    else:
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        (out, err) = process.communicate()
        return (out.decode('utf-8').split(linesep),
                err.decode('utf-8').split(linesep))


def safe_call(command):
    try:
        return(fab(command))
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Command failed: {}".format(err))
        raise(err)


def direct_remote_to_remote(src, dest):
    try:
        env.host_string = src.host
        src.to_local()
        return(single_remote_transfer(src, dest))
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Direct transfer from {} to {} on {} failed: {}".format(
              src.host, dest.host, src.host, err), )
        try:
            env.host_string = src.dest
            dest.to_local()
            return(single_remote_transfer(src, dest))
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print("Direct from {} to {} on {} failed: {}".format(src.host,
                                                                 dest.host,
                                                                 dest.host,
                                                                 err))
            raise(err)


def remote_proxy_transfer(src, dest):
    env.host_string = "localhost"
    try:
        cmd = ("ssh -L localhost:{p}:{dest}:22 {src} \"rsync -avz -e "
               "'ssh -p {p}' {src_p} {whoami}@locahost:{dest_p}\"")
        return(safe_call(cmd.format(p=env.bridge_port,
                         dest=dest.host,
                         src=src.host,
                         src_p=src.path,
                         whoami=env.whoami,
                         dest_p=dest.path)))
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print(("Transfer from {} --> localhost --> {}"
               " with rsync from {} failed: {}").format(
                   src.host, dest.host, src.host, err), )
        try:
            cmd = ("ssh -L localhost:{p}:{src}:22 {dest} \"rsync -avz -e "
                   "'ssh -p {p}' {whoami}@localhost:{src_p} {dest_p}\"")
            return(safe_call(cmd.format(p=env.bridge_port,
                             dest=dest.host,
                             src=src.host,
                             src_p=src.path,
                             whoami=env.whoami,
                             dest_p=dest.path)))
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print(("Transfer from {} --> localhost --> {}"
                   " with rsync from {} failed: {}").format(
                       src.host, dest.host, dest.host, err), )
            raise(err)


def double_remote_transfer(src, dest):
    try:
        return(direct_remote_to_remote(src, dest))
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Direct from {} to {} failed: {}".format(src.host,
                                                       dest.host,
                                                       err))
        try:
            return(remote_proxy_transfer(src, dest))
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print("Proxy transfer from {} to {} failed: {}".format(src.host,
                                                                   dest.host,
                                                                   err))
            raise(err)


def single_remote_transfer(src, dest):
    try:
        cmd = ("rsync -avz {} {}").format(src.target(), dest.target())
        return(safe_call(cmd))
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Direct transfer from {} to {} failed: {err}".format(src.host,
                                                                   dest.host,
                                                                   err))
        try:
            cmd = ("ssh -R {p}:localhost:22 {dest} \"rsync -avz -e "
                   "'ssh -p {p}' {whoami}@localhost:{src_p} {dest_p}\"")
            return(safe_call(cmd.format(p=env.bridge_port,
                             src=src.host,
                             dest=dest.host,
                             whoami=env.whoami,
                             src_p=src.path,
                             dest_p=dest.path)))
        except (FabricAbort, NetworkError, OSError, EOFError) as err:
            print("Reverse transcer from {} to {} failed: {}".format(src.host,
                                                                     dest.host,
                                                                     err))
            raise(err)


def local_transfer(src, dest):
    try:
        cmd = ("rsync -avz {} {}").format(src.target(), dest.target())
        return(safe_call(cmd))
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print("Local transfer from {} to {} failed: {}".format(src.path,
                                                               dest.path,
                                                               err))
        raise(err)


def parse_hostname_path(hostname_path):
    hostname = hostname_path.rstrip(path.sep)

    if ":" in hostname:
        host_parts = hostname.split(":")
        return(serverinfo(host=host_parts[0],
                          path=host_parts[1] + path.sep))
    else:
        return(serverinfo(host="localhost",
                          path=path.abspath(path.abspath(hostname) + path.sep)))


def main(source, destination):
    src = parse_hostname_path(source)
    dest = parse_hostname_path(destination)
    env.whoami = getuser()
    env.host_string = "localhost"
    env.proxy_host = "localhost"
    env.colorize_errors = True
    env.use_ssh_config = True
    env.bridge_port = '4326'
    paramiko.util.log_to_file("/dev/null")

    try:
        if src.host == "localhost" or dest.host == "localhost":
            if src.host == "localhost" and dest.host == "localhost":
                print(local_transfer(src, dest))
            else:
                print(single_remote_transfer(src, dest))
        else:
            print(double_remote_transfer(src, dest))
    except (FabricAbort, NetworkError, OSError, EOFError) as err:
        print('All methods of transfer failed...')
        raise(err)


if __name__ == "__main__":
    if len(argv) == 3:
        main(argv[1], argv[2])
    else:
        print(__USAGE_STATEMENT)
