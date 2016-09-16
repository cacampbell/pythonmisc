#!/usr/bin/env python3
from BQSR import BQSR
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    bqsr = BQSR(*args, **kwargs)
    jobs = bqsr.run()
    return (jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
