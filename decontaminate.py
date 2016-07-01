#!/usr/bin/env python3
from Decontaminator import Decontaminator
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    decontam = Decontaminator(*args, **kwargs)
    decontam.modules = ['java', 'slurm']
    return (decontam.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
