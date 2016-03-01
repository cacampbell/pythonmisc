#!/usr/bin/env python
from __future__ import print_function
import os
from subprocess import call
import paramiko
import re
import errno
from multiprocessing.dummy import Pool
from functools import partial

# add a step to make local directories that need to be made before dispatching
# the rsync commands.

# Add configuration so that the remote and local is automatically figured out
# behind the scenes -- so list all of the local files and make the appropriate
# directories remotely, or get all remote files and make the local directories
# locally. This can probably be done easily enough with fabric, maybe using
# the rsync process as a more general part of the fabfile.

# So, stage 1 will be to get this working with remote to local multithreaded
# using paramiko, then stage 2 with be the incorporation of more general
# multithreaded processes into a fabfile for server administration / system
# administration tasks -- could definitely set up two functions / command line
# options in fabric for transfer of all root X to remote root Y, or all remote
# root Y to root X.

max_processes = 6
remote_root = "/group/nealedata4/pinus_armandii_wgs"
local_root = "/raid10/data/pinus_armandii_wgs"
ssh_options = {'hostname': 'farm'}


def dispatch_commands(cmds):
    pool = Pool(max_processes)
    for i, returncode in enumerate(pool.imap(partial(call, shell=True), cmds)):
        if returncode != 0:
            print("{} command failed: {}".format(i, returncode))


def get_client(options):
    client = paramiko.SSHClient()
    client._policy = paramiko.WarningPolicy()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_config = paramiko.SSHConfig()
    user_config_file = os.path.expanduser("~/.ssh/config")

    if os.path.exists(user_config_file):
        with open(user_config_file, 'r+') as f:
            ssh_config.parse(f)

    cfg = {'hostname': options['hostname'], 'password': options['password']}
    user_config = ssh_config.lookup(cfg['hostname'])

    if 'hostname' in user_config:
        cfg['hostname'] = user_config['hostname']

    if 'user' in user_config:
        cfg['username'] = user_config['user']

    if 'port' in user_config:
        cfg['port'] = user_config['port']

    if 'identityfile' in user_config:
        cfg['key_filename'] = user_config['identityfile']

    if 'proxycommand' in user_config:
        cfg['sock'] = paramiko.ProxyCommand(user_config['proxycommand'])

    client.connect(**cfg)
    return client


def get_remote_files(client, directory):
    (stdin, stdout, stderr) = client.exec_command(
        "find {} -type f | sed -n 's|^{}/||p'".format(directory, directory))
    return stdout.readlines()


def rsync_commands(filelist):
    cmds = []

    for filename in filelist:
        remotef = os.path.join(remote_root, filename)
        local_file = os.path.join(local_root, filename)
        local = os.path.dirname(local_file)
        cmd = ('rsync -avzP farm:{source} {dest}').format(dest=local,
                                                          source=remotef)
        cmd = re.sub("\r\n?|\n", '', cmd)
        cmds += [cmd]

    return cmds


def make_directories(list_of_directories):
    for folder in list_of_directories:
        abs_folder = os.path.join(local_root, folder)
        try:
            print(abs_folder)
            os.makedirs(abs_folder)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(abs_folder):
                pass


def main():
    client = get_client(ssh_options)
    files = get_remote_files(client, remote_root)
    directories = set([os.path.dirname(x) for x in files])
    make_directories(directories)
    commands = rsync_commands(files)
    dispatch_commands(commands)

if __name__ == "__main__":
    main()
