#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess
import re
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from module_loader import module


module_args = ['java']
suffix = ".fq.gz"
job_prefix = "BBDuk_"
partition = "bigmemm"
maxmem = "100"
maxcpu = "12"
input_root = ""
output_root = ""
adapters = ""
email = "cacampbell@ucdavis.edu"
dry_run = True
verbose = True


def dispatch_to_slurm(commands):
    scripts = {}

    for job_name, command in commands.iteritems():
        script = submit(command, job_name=job_name, time="0",
                        memory="{}G".format(maxmem), backend="slurm",
                        shell_script="#!/usr/bin/env bash")
        script += " --partition={}".format(partition)
        script += " --ntasks=1"
        script += " --cpus-per-task={}".format(maxcpu)
        script += " --mail-type=END,FAIL"
        script += " --mail-user={}".format(email)
        scripts[job_name] = script

    scheduled_jobs = set(queued_or_running_jobs())

    for job_name, script in scripts.iteritems():
        if job_name not in scheduled_jobs:
            if verbose:
                print("{}".format(script), file=sys.stdout)

            if not dry_run:
                subprocess.call(script, shell=True)
        else:
            print("{} already running, skipping".format(job_name),
                  file=sys.stderr)


def remove_ends(filelist):
    new_filelist = []

    for filename in filelist:
        new_name = re.sub("R[0-9]", "R#", filename)
        new_filelist += [new_name]

    return set(new_filelist)


def make_commands(filenames):
    commands = {}
    filenames = remove_ends(filenames)

    for filename in filenames:
        job_name = job_prefix + "{}".format(os.path.basename(filename))
        output_f = os.path.join(output_root, os.path.basename(filename))
        command = ("bbduk.sh in={filename} out={output} minlen=25 qtrim=rl "
                   "trimq=10 ktrim=r k=25 mink=11 ref={adpts} hdist=1 tpe "
                   "tbo maq=10 mlf=50").format(filename=filename,
                                               output=output_f,
                                               adpts=adapters)
        commands[job_name] = command

        if verbose:
            print(command, file=sys.stdout)

    return commands


def make_directories():
    input_base = os.path.dirname(input_root)
    command = ("find {} -type d | sed -n 's|{}||p' | "
               "parallel --gnu -j 4 mkdir -p {}/{{}}").format(input_root,
                                                              input_base,
                                                              output_root)
    subprocess.call(command, shell=True)


def get_files(directory):
    filelist = []

    for root, directories, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(suffix):
                abs_path = os.path.join(root, filename)

                if verbose:
                    print(abs_path, file=sys.stdout)

                filelist += [abs_path]

    return filelist


def load_modules(module_args):
    try:
        args = ['load']
        args.extend(module_args)
        module(args)
    except Exception as e:
        print("Could not load module: {}".format(e), file=sys.stderr)


def main(root):
    if verbose:
        print("Loading Modules...", file=sys.stdout)

    load_modules(module_args)

    if verbose:
        print("Gathering Files...", file=sys.stdout)

    filenames = get_files(root)

    if verbose:
        print("Making output directories...", file=sys.stdout)

    make_directories()

    if verbose:
        print("Making Commands...", file=sys.stdout)

    commands = make_commands(filenames)

    if verbose:
        print("Dispatching to Slurm...", file=sys.stdout)

    dispatch_to_slurm(commands)


if __name__ == "__main__":
    main(input_root)
