#!/usr/bin/env python
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from module_loader import module
import os
import subprocess
import sys


job_prefix = "picard_index_"
module_args = ['java']
initial_heap = "340"
max_heap = str(int(initial_heap) + 10)
max_memory = str(int(max_heap) + 10)


def dispatch(commands):
    scripts = {}

    for job_name, command in commands.iteritems():
        script = submit(command, job_name=job_name,
                        time="0", memory=max_memory + "G", backend="slurm",
                        shell_script="#!/usr/bin/env bash")
        script = script + " --partition=bigmemm"
        scripts[job_name] = script

    scheduled_jobs = set(queued_or_running_jobs())

    for job_name, script in scripts.iteritems():
        if job_name not in scheduled_jobs:
            sys.stdout.write("\n{}\n".format(script))
            subprocess.call(script, shell=True)
        else:
            sys.stderr.write("{} running, skipping.\n".format(job_name))


def make_commands(filelist):
    commands = {}
    picard = "/home/cacampbe/picardtools/picard-tools-2.0.1/picard.jar"
    for filename in filelist:
        job_name = job_prefix + "{}".format(filename)
        command = "java -Xms{}G -Xmx{}G -jar {} BuildBamIndex INPUT={} \
            MAX_RECORDS_IN_RAM={}".format(initial_heap, max_heap,
                                          picard, filename,
                                          str(int(max_memory) / 10))
        commands[job_name] = command

    return commands


def get_files(directory):
    filelist = []

    for root, directories, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(".sorted.dedup.bam"):
                abs_path = os.path.join(root, filename)
                filelist.append(abs_path)

    return filelist


def load_module(module_args):
    try:
        modules = ['load']
        modules.extend(module_args)
        module(modules)
    except Exception as e:
        sys.stdout.write("Could not load module {}\n\
                         This may result in a runtime error in the \
                         dispatched slurm jobs!\n".format(str(modules)))
        sys.stdout.write("Exception: {}\n".format(str(e)))
        raise e


def main(directory):
    filelist = get_files(directory)
    load_module(module_args)
    commands = make_commands(filelist)
    dispatch(commands)


if __name__ == "__main__":
    main(os.getcwd())
