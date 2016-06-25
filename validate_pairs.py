#!/usr/bin/env python3
from Repair import Repair
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    repair = Repair(*args, **kwargs)
    return (repair.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
