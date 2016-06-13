#!/usr/bin/env python3
from Decontaminator import Decontaminator
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    decontaminator = Decontaminator(*args, **kwargs)
    return(decontaminator.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
