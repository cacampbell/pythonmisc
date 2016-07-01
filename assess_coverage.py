#!/usr/bin/env python3
from Coverage import Coverage
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    cover = Coverage(*args, **kwargs)

    if "extension" not in kwargs.keys():
        cover.extension = r".bam"

    if "read_regex" not in kwargs.keys():
        cover.read_regex = r"_pe"

    jobs = cover.run()
    return (jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
