#!/usr/bin/env python3
from RepairReads import RepairReads

from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    repair = RepairReads(*args, **kwargs)
    repair.modules = ['java']
    jobs = repair.run()
    return (jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
