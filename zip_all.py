#!/usr/bin/env python3
from ZipAll import ZipAll
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    zipper = ZipAll(*args, **kwargs)
    return (zipper.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
