#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess
import re
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
from module_loader import module


input_root = "/group/nealedata4/Piar_wgs/Cleaner"
output_root = "/group/nealedata4/Piar_wgs/Cleaner_decontam"
rerun_files = []
module_args = ['java']
suffix = ".fq.gz"
job_prefix = "BBMap_"
partition = "bigmemh"
maxmem = "64"
maxcpu = "12"
javaxmx = str(int(0.85 * int(maxmem)))
javaxms = str(int(javaxmx) - 4)
javathreads = str(int(maxcpu) - 2)
masked_human_ref = ("/group/nealedata4/Psme_reseq/qc/Hosa_masked/"
                    "hg19_main_mask_ribo_animal_allplant_allfungus.fa.gz")
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


def make_directories():
    command = ("find {} -type d | sed -n 's|{}||p' | "
               "parallel --gnu -j 4 mkdir -p {}/{{}}").format(input_root,
                                                              input_root,
                                                              output_root)
    subprocess.call(command, shell=True)


def make_commands(filenames):
    commands = {}
    filenames = [x for x in filenames if "_1" in x]

    for filename in filenames:
        job_name = job_prefix + "{}".format(os.path.basename(filename))
        input_f1 = filename
        input_f2 = re.sub("_1", "_2", filename)
        output_f1 = output_file(input_f1)
        output_f2 = output_file(input_f2)
        human_f1 = re.sub(suffix, ".human.fq.gz", output_f1)
        human_f2 = re.sub(suffix, ".human.fq.gz", output_f2)
        stats_f = re.sub(suffix, ".stats.txt", output_f1)
        stats_f = re.sub("_1", "pe", stats_f)
        command = ("bbmap.sh -Xms{inith}G -Xmx{maxh}G minid=0.95 maxindel=3 "
                   "bwr=0.16 bw=12 quickmatch fast minhits=2 ref={r} nodisk "
                   "in1={i1} in2={i2} outu1={o1} outu2={o2} outm1={h1} "
                   "outm2={h2} statsfile={s} "
                   "usejni=t threads={t}").format(inith=javaxms,
                                                  maxh=javaxmx,
                                                  r=masked_human_ref,
                                                  i1=input_f1,
                                                  i2=input_f2,
                                                  o1=output_f1,
                                                  o2=output_f2,
                                                  h1=human_f1,
                                                  h2=human_f2,
                                                  s=stats_f,
                                                  t=javathreads)

        if not existing_files_check([output_f1,
                                    output_f2,
                                    human_f1,
                                    human_f2,
                                    stats_f]):
            if rerun_files != []:
                for rerun in rerun_files:
                    if re.search(rerun, command):
                        commands[job_name] = command
                        if verbose:
                            print(command)
                    else:
                        print("Not rerunning {}".format(job_name))
            else:
                commands[job_name] = command
                if verbose:
                    print(command)
        else:
            print("{} already ran, skipping".format(job_name),
                  file=sys.stderr)
    return commands


def get_files(directory):
    filelist = []

    for input_root, directories, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(suffix):
                abs_path = os.path.join(input_root, filename)

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


def main(input_root):
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
    main(input_root)
