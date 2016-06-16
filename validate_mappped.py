#!/usr/bin/env python3
from ValidateMapped import ValidateMapped

from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    validator = ValidateMapped(*args, **kwargs)

    if "extension" not in kwargs.keys():
        validator.extension = r".sam"

    if "read_regex" not in kwargs.keys():
        validator.read_regex = r"_pe"

    jobs = validator.run()
    return (jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
