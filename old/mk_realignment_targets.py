#!/usr/bin/env python
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from re import sub
from module_loader import module
import os
import subprocess
import sys


gatk = "/home/cacampbe/gatk-protected/target/GenomeAnalysisTK.jar"
reference_genome = \
    "/group/nealedata/databases/Pila/genome/v1.0/pila.v1.0.scafSeq.fa"
file_suffixes = ".bwamem.sorted.filtered.bam"
job_prefix = "create_realign_targets_"
modules_args = ["java"]
init_heap = "12"  # Start slightly lower than max heap
max_heap = str(int(init_heap) + 4)  # Add more to account for operation overhead
max_memory = str(int(max_heap) + 4)  # memory to account for java process total
max_records_in_ram = str(25000 * int(max_heap))  # 1/10 recommended by GATK


def dispatch(commands):
    scripts = {}

    for job_name, command in commands.iteritems():
        script = submit(command, job_name=job_name,
                        time="0", memory=max_memory + "G", backend="slurm",
                        shell_script="#!/usr/bin/env bash")
        script = script + " --partition=bigmemh"
        scripts[job_name] = script

    scheduled_jobs = set(queued_or_running_jobs())

    for job_name, script in scripts.iteritems():
        if job_name not in scheduled_jobs:
            sys.stdout.write("\n{}\n".format(script))
            subprocess.call(script, shell=True)
        else:
            sys.stderr.write("{} running, skipping\n".format(job_name))


def make_commands(filelist):
    commands = {}

    for filename in filelist:
        job_name = job_prefix + "{}".format(os.path.basename(filename))
        out = sub(file_suffixes, ".realignment_targets.list", filename)
        cmd = "java -Xms{xms}G -Xmx{xmx}G -jar {jar} -T RealignerTargetCreator \
            -R {reference} \
            -I {filename} \
            -o {output}".format(xms=init_heap, xmx=max_heap, jar=gatk,
                                reference=reference_genome, filename=filename,
                                output=out)
        commands[job_name] = cmd

    return commands


def get_files(directory):
    filelist = []

    for root, directories, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(file_suffixes):
                abs_path = os.path.join(root, filename)
                filelist.append(abs_path)

    return filelist


def load_module(modules):
    try:
        args = ['load']
        args.extend(modules)
        module(args)
    except Exception as e:
        sys.stdout.write("""Could not load module(s) {}.
                         This may result in a runtime error!
                         """.format(str(modules)))
        sys.stdout.write("Exception: {}\n".format(str(e)))
        raise e


def main(directory):
    load_module(modules_args)
    filelist = get_files(directory)
    commands = make_commands(filelist)
    dispatch(commands)


if __name__ == "__main__":
    main(os.getcwd())
