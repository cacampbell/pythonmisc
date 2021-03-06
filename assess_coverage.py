#!/usr/bin/env python3
from Coverage import Coverage
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    cover = Coverage(*args, **kwargs)
    jobs = cover.run()
    return (jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
