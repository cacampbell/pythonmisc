#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess
import re
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from module_loader import module

input_root = "/group/nealedata4/Psme_reseq/clean_decontam/decontam/"
output_root = "/group/nealedata5/Psme_reseq/mapped"
module_args = ['java']
suffix = ".fq.gz"
job_prefix = "BBMap_"
partition = "bigmemm"
maxmem = "300"
maxcpu = "30"
reference = "/group/nealedata4/Psme_reseq/genome/Psme.scf.uniq.fa"
javaxmx = str(int(maxmem) - 2)
javathreads = str(int(maxcpu) - 2)
email = "cacampbell@ucdavis.edu"
dry_run = False
verbose = False


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


def existing_files_check(list_of_files):
    for filename in list_of_files:
        if os.path.isfile(filename):
            return True

    return False


def output_file(filename):
    return os.path.join(output_root, os.path.relpath(filename,
                                                     start=input_root))


def make_commands(filenames):
    commands = {}
    filenames = [x for x in filenames if "R1" in x]

    for filename in filenames:
        job_name = job_prefix + "{}".format(os.path.basename(filename))
        input_f1 = filename
        input_f2 = re.sub("R1", "R2", filename)
        map_sam = re.sub("R1", "pe", filename)
        map_sam = re.sub(suffix, ".sam", map_sam)
        map_sam = output_file(map_sam)
        unmap_sam = re.sub(".sam", ".unmapped.sam", map_sam)
        covstat = re.sub("R1", "covstats", filename)
        covstat = re.sub(suffix, ".txt", covstat)
        covstat = output_file(covstat)
        covhist = re.sub("R1", "covhist", filename)
        covhist = re.sub(suffix, ".txt", covhist)
        covhist = output_file(covhist)
        basecov = re.sub("R1", "basecov", filename)
        basecov = re.sub(suffix, ".txt", basecov)
        basecov = output_file(basecov)
        bincov = re.sub("R1", "bincov", filename)
        bincov = re.sub(suffix, ".txt", filename)
        bincov = output_file(bincov)
        bashscript = re.sub("R1", "sort_index", filename)
        bashscript = re.sub(suffix, '.sh', bashscript)
        bashscript = output_file(bashscript)
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} ref={r} "
                   "nodisk covstats={covstat} covhist={covhist} threads={t} "
                   "slow k=12 -Xmx{xmx}G basecov={basecov} bincov={bincov} "
                   "bamscript={bs}; source {bs}").format(i1=input_f1,
                                                         i2=input_f2,
                                                         om=map_sam,
                                                         ou=unmap_sam,
                                                         r=reference,
                                                         covstat=covstat,
                                                         covhist=covhist,
                                                         basecov=basecov,
                                                         bincov=bincov,
                                                         xmx=javaxmx,
                                                         t=javathreads,
                                                         bs=bashscript)

        if not existing_files_check([map_sam, unmap_sam, covstat, covhist,
                                     covhist, basecov, bincov]):
            commands[job_name] = command
        else:
            print("{} already ran, skipping".format(job_name),
                  file=sys.stderr)

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


def main():
    if verbose:
        print("Loading Modules...", file=sys.stdout)

    load_modules(module_args)

    if verbose:
        print("Gathering Files...", file=sys.stdout)

    filenames = get_files(input_root)

    if verbose:
        print("Making output directories...")

    make_directories()

    if verbose:
        print("Making Commands...", file=sys.stdout)

    commands = make_commands(filenames)

    if verbose:
        print("Dispatching to Slurm...", file=sys.stdout)

    dispatch_to_slurm(commands)


if __name__ == "__main__":
    main()
