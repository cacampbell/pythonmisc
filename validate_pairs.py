#!/usr/bin/env python3
from Repair import Repair
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    repair = Repair(*args, **kwargs)
    repair.modules = ['slurm', 'java/1.8']
    return (repair.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
