#!/usr/bin/env python3
from ReduceReads import ReduceReads
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    rr = ReduceReads(*args, **kwargs)
    return (rr.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
