#!/usr/bin/env python3
from CleanSort import CleanSort
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    merger = CleanSort(*args, **kwargs)
    return (merger.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
