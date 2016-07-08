#!/usr/bin/env python3
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

cluster_options = {}  # options for each step (keys)
commands = {}  # commands, with keys = step
steps = []  # Deterministic list of keys to insure things are run in order


def make_commands(mode="DNA",
                  deduplicate=False,
                  normalize=False,
                  input_root="Raw",
                  job_name="PreProc",
                  partition="bigmemm",
                  email_address="cacampbell@ucdavis.edu",
                  email_options="BEGIN,END,FAIL"):
    # TODO move this to function
    reformat_options = {}
    reformat_cluster_options = deepcopy(default_options)
    reformat_cluster_options["memory"] = "180G"
    reformat_cluster_options["cpus"] = "18"
    reformat_cluster_options["partition"] = partition
    reformat_cluster_options["email_address"] = email_address
    reformat_cluster_options["email_options"] = email_options
    reformat_options["cluster_options"] = reformat_cluster_options

    reformat_options["input_root"] = input_root
    reformat_options["output_root"] = "Cleaner"
    cluster_options["reformat"] = reformat_options

    # TODO: each option expected needs to be in string with key name
    commands["reformat"] = ("quality_control.py "
                            "--input_root={input_root} "
                            "--output_root={output_root}").format(
        **cluster_options["reformat"])

    steps += ["reformat"]

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

    for step in steps:
        # This should cycle through each 'step' in the process, collect the jobs
        # from each step submission and the output of the submitted jobs of that
        # step, then use those as dependies for the next step in the process,
        # until there are no more steps.
        if jobs:
            cluster_options[step]["cluster_options"]["depends_on"] = jobs

        jobs = run_step(commands[step], options=cluster_options[step])


if __name__ == "__main__":
    run_with_args(main)
