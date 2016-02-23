#!/usr/bin/env python
from __future__ import print_function
import os
from subprocess import call
import paramiko
import re
from multiprocessing.dummy import Pool
from functools import partial

remote_directory = ("/group/nealedata4/pinus_armandii_wgs/Clean_Alignments/"
                    "BAM_Sorted_Filtered")
max_processes = 6
dest = "/raid10/data/pinus_armandii_wgs/BAM_Sorted_Filtered"
ssh_options = {'hostname': 'farm',
               'password': "6529Campbell'ssoup89"}


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
    (stdin, stdout, stderr) = client.exec_command('ls {}'.format(directory))
    return stdout.readlines()


def rsync_commands(filelist):
    cmds = []

    for filename in filelist:
        remotef = os.path.join(remote_directory, filename)
        cmd = ('rsync -avzrPe "ssh" farm:{remote} {local}/').format(local=dest,
                                                           remote=remotef)
        cmd = re.sub("\r\n?|\n", '', cmd)
        cmds += [cmd]
        print(cmd)

    print(cmds)
    return cmds


def main():
    client = get_client(ssh_options)
    files = get_remote_files(client, remote_directory)
    commands = rsync_commands(files)
    dispatch_commands(commands)

if __name__ == "__main__":
    main()
