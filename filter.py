#!/usr/bin/env python
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from re import sub
import module_loader
import os
import subprocess
import sys


max_memory = "100"
job_prefix = "filter_"
modules = ["samtools"]


def dispatch(commands):
    scripts = {}

    for filename, command in commands.iteritems():
        job_name = job_prefix + "{}".format(filename)
        script = submit(command, job_name=job_name,
                        time="0", memory=max_memory + "G", backend="slurm",
                        shell_script="#!/usr/bin/env bash")
        script = script + " --partition=bigmemh"
        scripts[job_name] = script

    scheduled_jobs = set(queued_or_running_jobs())

    for job_name, script in scripts.iteritems():
        if job_name not in scheduled_jobs:
            sys.stdout.write("\n%s\n" % script)
            subprocess.call(script, shell=True)
        else:
            sys.stderr.write("{} running, skipping\n".format(job_name))


def make_commands(filelist):
    commands = {}

    for filename in filelist:
        output = sub(".bwamem.sorted.bam$", ".bwamem.sorted.filtered.bam",
                     filename)
        command = "samtools view -F 0x04 -f 0x02 -bq 1 {} > {}".format(filename,
                                                                       output)
        # http://www.htslib.org/doc/samtools.html
        # -f 0x02 Only output alignments with all bits set in 0x02 present in
        # the FLAG field
        # -F 0x04 Do not output alignments with any bits set in 0x04 present in
        # the FLAG field.
        commands[filename] = command

    return commands


def get_files(directory):
    filelist = []

    for root, directories, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith("bwamem.sorted.bam"):
                abs_path = os.path.join(root, filename)
                filelist.append(abs_path)

    return filelist


def load_module(modules):
    try:
        module_loader.module('load', str(modules))
    except Exception as e:
        sys.stdout.write("""Could not load module {}.
                         This may result in a runtime error!
                         """.format(modules))
        sys.stdout.write("Exception: {}\n".format(str(e)))


def main(directory):
    load_module(modules)
    filelist = get_files(directory)
    commands = make_commands(filelist)
    files = [os.path.basename(filename) for filename in filelist]
    dispatch(commands)


if __name__ == "__main__":
    main(os.getcwd())
