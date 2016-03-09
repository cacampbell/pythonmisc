#!/usr/bin/env python
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from re import sub
from module_loader import module
import os
import subprocess
import sys
# import math
# important: make sure that configuration uses more memory from slurm than the
# max heap space given to the java VM -- java still needs memory to run that is
# apart from the process memory


picard = "/home/cacampbe/picardtools/picard-tools-2.0.1/picard.jar"
file_suffixes = ".bwamem.sorted.filtered.bam"
job_prefix = "markduplicates_"
modules_args = ["java"]
init_heap = "12"  # Start slightly lower than max heap
max_heap = str(int(init_heap) + 4)  # Add more to account for operation overhead
max_memory = str(int(max_heap) + 4)  # memory to account for java process total
# ncpu = str(int(math.floor(int(max_memory) / 8)))  # 8G per CPU on cluster
max_records_in_ram = str(25000 * int(max_heap))  # 1/10 recommended by GATK


def dispatch(commands):
    scripts = {}

    for job_name, command in commands.iteritems():
        script = submit(command, job_name=job_name,
                        time="0", memory=max_memory + "G", backend="slurm",
                        shell_script="#!/usr/bin/env bash")
        script = script + " --partition=bigmemh"
        # script = script + " --cpus-per-task={}".format(ncpu)
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
        out = sub(file_suffixes, ".bwamem.sorted.filtered.dedup.bam", filename)
        metrics = sub("bam", "metrics.txt", out)
        cmd = "java -Xms{xms}G -Xmx{xmx}G -jar {jar} MarkDuplicates INPUT={inf}\
            OUTPUT={out} REMOVE_DUPLICATES=true \
            MAX_RECORDS_IN_RAM={max_records} ASSUME_SORTED=true \
            METRICS_FILE={metrics}".format(xms=init_heap, xmx=max_heap,
                                           jar=picard,
                                           inf=filename, out=out,
                                           max_records=max_records_in_ram,
                                           metrics=metrics)
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
