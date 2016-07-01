#!/usr/bin/env python3
from MergeReads import MergeReads
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    merger = MergeReads(*args, **kwargs)
    return (merger.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
