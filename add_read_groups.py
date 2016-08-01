#!/usr/bin/env python3
from AddReadGroups import ReadGrouper
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    rg = ReadGrouper(*args, **kwargs)
    return(rg.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
