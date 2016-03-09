#!/usr/bin/env python
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from module_loader import module
from re import sub
import os
import subprocess
import sys


job_prefix = "picard_index_"
module_args = ['java']
initial_heap = "340"
reference_genome = "/share/nealedata/databases/Pila/bwa/v1.0/pila.v1.0.scafSeq"
gatk = "/home/cacampbe/gatk-protected/target/GenomeAnalysisTK.jar"
file_suffixes = ".bwamem.sorted.filtered.bam"
# ".bwamem.sorted.filtered.dedup.bam"
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
    for filename in filelist:
        job_name = job_prefix + "{}".format(filename)
        target_file = sub(file_suffixes, ".realignment_targets.list", filename)
        output_file = sub(file_suffixes,
                          ".bwamem.sorted.filtered.dedup.realigned.bam")
        command = "java -Xms{xms}G -Xmx{xmx}G -jar {jar} -T IndelRealigner \
            -R {reference} \
            -I {filename} \
            -targetIntervals {target} \
            -o {output}".format(xms=initial_heap, xmx=max_heap, jar=gatk,
                                reference=reference_genome, filename=filename,
                                target=target_file, output=output_file)
        commands[job_name] = command

    return commands


def get_files(directory):
    filelist = []

    for root, directories, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(file_suffixes):
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
