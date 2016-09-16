#!/usr/bin/env python3
from AssessMappingQuality import Assesser
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    a = Assesser(*args, **kwargs)
    jobs = a.run()
    return (jobs)


if __name__ == "__main__":
    run_parallel_command_with_args(main)
