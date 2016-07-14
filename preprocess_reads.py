#!/usr/bin/env python3
from collections import OrderedDict
from copy import deepcopy

from parallel_command_parse import run_with_args

# TODO: The following
# global cluster options defualt
# modify cluster options copy for each job
# run_step will submit a job, gather its jobs, then return the total set of
# jobs needed for the next step
# Write each command using cluster_options copies and whichever params are needed

default_options = {'memory': '2G',
                   'nodes': '1',
                   'cpus': '2',
                   'partition': 'bigmemm',
                   'job_name': 'PreProc',
                   'email_address': 'cacampbell@ucdavis.edu',
                   'email_options': 'BEGIN,END,FAIL'}

cluster_options = OrderedDict()  # options for each step (keys)
commands = OrderedDict()  # commands, with keys = step


def update_cluster_options(memory,
                           cpus,
                           job_name,
                           partition,
                           email_address,
                           email_options):
    options = {}
    cluster_options = deepcopy(default_options)
    cluster_options["job_name"] = job_name
    cluster_options["memory"] = memory
    cluster_options["cpus"] = cpus
    cluster_options["partition"] = partition
    cluster_options["email_address"] = email_address
    cluster_options["email_options"] = email_options
    options["cluster_options"] = cluster_options
    return options


def make_commands(mode="DNA",
                  deduplicate=False,
                  normalize=False,
                  input_root="Raw",
                  job_name="PreProc",
                  partition="bigmemm",
                  email_address="cacampbell@ucdavis.edu",
                  email_options="BEGIN,END,FAIL"):
    qualitycontrol_options = update_cluster_options("200G", "18", job_name,
                                                    partition,
                                                    email_address,
                                                    email_options)
    qualitycontrol_options["input_root"] = input_root
    qualitycontrol_options["output_root"] = "Cleaner"
    qualitycontrol_options["ref"] = "~/.prog/bbmap/resources/adapters.fa"
    cluster_options["reformat"] = qualitycontrol_options

    # TODO: each option expected needs to be in string with key name
    commands["quality_control"] = ("quality_control.py "
                                   "--cluster_options={cluster_options}"
                            "--input_root={input_root} "
                                   "--output_root={output_root} "
                                   "--reference={ref}").format(
        **cluster_options["quality_control"])

    if mode.upper().strip() == "RNA":
        commands["syntehtic_molecule_removal"] = ("")
        # TODO: continue

    if deduplicate:
        pass

    if normalize:
        pass


def run_step(job, options):
    return ""  # TODO


def main(*args, **kwargs):
    make_commands(*args, **kwargs)
    jobs = ""

    for step in commands.keys():
        # This should cycle through each 'step' in the process, collect the jobs
        # from each step submission and the output of the submitted jobs of that
        # step, then use those as dependies for the next step in the process,
        # until there are no more steps.
        if jobs:
            cluster_options[step]["cluster_options"]["depends_on"] = jobs

        jobs = run_step(commands[step], options=cluster_options[step])


if __name__ == "__main__":
    run_with_args(main)
