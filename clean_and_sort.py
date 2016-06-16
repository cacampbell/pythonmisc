#!/usr/bin/env python3
from CleanSort import CleanSort
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    merger = CleanSort(*args, **kwargs)
    merger.modules = ['java', 'slurm']
    merger.exclusions = "unmapped,unmap"
    return (merger.run())


if __name__ == "__main__":
    run_parallel_command_with_args(main)
