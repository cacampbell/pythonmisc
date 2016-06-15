#!/usr/bin/env python3
from DecontaminateByMapping import DecontaminateByMapping
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    rh = DecontaminateByMapping(*args, **kwargs)
    rh.modules = ['java', 'slurm']
    jobs = rh.run()
    return(jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
