#!/usr/bin/env python3
from Splitter import Splitter
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    s = Splitter(*args, **kwargs)
    return (s.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
