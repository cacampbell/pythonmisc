#!/usr/bin/env python3
from MergeBAMAlignment import MergeBamAlignment
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    merger = MergeBamAlignment(*args, **kwargs)
    merger.modules = ['java', 'slurm']
    return (merger.run())


if __name__ == "__main__":
    run_parallel_command_with_args(main)
