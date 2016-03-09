#!/usr/bin/env python
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from re import sub
import os
import subprocess
import sys


def dispatch(filelist, commands):
    scripts = []

    for i, command in enumerate(commands):
        script = submit(command, job_name="samtools_sort_%s" % filelist[i],
                        time="0", memory=240000, backend="slurm",
                        shell_script="#!/usr/bin/env bash")
        script = script + " --cpus-per-task=12"
        script = script + " --ntasks=1"
        script = script + " --partition=bigmemm"
        scripts.append(script)

    scheduled_jobs = set(queued_or_running_jobs())

    for i, script in enumerate(scripts):
        if "samtools_sort_%s" % filelist[i] not in scheduled_jobs:
            sys.stdout.write("\n%s\n" % script)
            subprocess.call(script, shell=True)
        else:
            sys.stderr.write("Job name 'samtools_sort_%s' found in queued or \
                             running jobs list" % filelist[i])


def make_commands(filelist):
    commands = []
    for filename in filelist:
        out_prefix = sub(".bam$", ".sorted", filename)
        out_file = out_prefix + ".bam"
        command = "/home/cacampbe/samtools_programs/bin/samtools sort -T %s -l \
            9 -@ 12 -m 8G -O bam -o %s %s" % (out_prefix, out_file, filename)
        commands.append(command)

    return commands


def get_files(directory):
    filelist = []

    for root, directories, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(".bam"):
                abs_path = os.path.join(root, filename)
                filelist.append(abs_path)

    return filelist


def main(directory):
    filelist = get_files(directory)
    commands = make_commands(filelist)
    files = [os.path.basename(filename) for filename in filelist]
    dispatch(files, commands)


if __name__ == "__main__":
    main(os.getcwd())
