#!/usr/bin/env python
from __future__ import print_function

import subprocess
import sys

import os
import re
from clusterlib.scheduler import queued_or_running_jobs
from clusterlib.scheduler import submit

from module_loader import module

fastqc = "/home/cacampbe/fastqc/FastQC/fastqc"
max_memory = "18G"
max_cpus = "2"
partition = "bigmemm"
email = "cacampbell@ucdavis.edu"
job_prefix = "FastQC_"
suffix = ".fq.gz"


def output_exists(script):
    tokens = script.split(' ')
    basename = os.path.basename(tokens[1])
    basename = re.sub(suffix, '', basename)

    for token in tokens:
        if re.search("outdir", token):
            filepath = re.sub("--outdir=", '', token)
            if os.path.isdir(os.path.join(filepath, basename)):
                return True

    return False


def dispatch(scripts):
    running = set(queued_or_running_jobs())

    for job_name, script in scripts.iteritems():
        if job_name not in running and not output_exists(script):
            subprocess.call(script, shell=True)
        else:
            print("Job already underway or completed: {}".format(job_name),
                  file=sys.stderr)


def write_scripts(commands):
    scripts = {}

    for job_name, command in commands.iteritems():
        script = submit(command,
                        job_name=job_name,
                        time="0",
                        memory=max_memory,
                        backend="slurm",
                        shell_script="#!/usr/bin/env bash")
        script += " --partition={}".format(partition)
        script += " --ntasks=1"
        script += " --cpus-per-task={}".format(max_cpus)
        script += " --mail-type=END,FAIL"
        script += " --mail-user={}".format(email)
        scripts[job_name] = script

    return scripts


def make_commands(outf, filenames):
    commands = {}

    for filename in filenames:
        job_name = "{}{}".format(job_prefix, os.path.basename(filename))
        command = "{} {} --outdir={}".format(fastqc, filename, outf)
        commands[job_name] = command

    return commands


def get_files(directory):
    filenames = []

    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(suffix):
                filenames += [os.path.join(root, filename)]

    return filenames


def load_modules(modules):
    try:
        args = ['load']
        args.extend(modules)
        module(args)
    except Exception as e:
        print("Could not load module(s): {}".format(modules), file=sys.stderr)
        print("Exception: {}".format(e))


def main():
    input_root = sys.argv[1]
    output_root = sys.argv[2]
    print("Loading modules...")
    load_modules('java')
    print("Getting files....")
    files = get_files(input_root)
    print("Writing format_commands...")
    commands = make_commands(output_root, files)
    print("Writing write_scripts...")
    scripts = write_scripts(commands)
    print("Dispatching write_scripts...")
    dispatch(scripts)


if __name__ == "__main__":
    main()
