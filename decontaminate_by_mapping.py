#!/usr/bin/env python
from DecontaminateByMapping import DecontaminateByMapping
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    rh = DecontaminateByMapping(*args, **kwargs)
    rh.modules = ['java', 'slurm']
    rh.run()


if __name__ == "__main__":
    run_parallel_command_with_args(main)
