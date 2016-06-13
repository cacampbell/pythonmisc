#!/usr/bin/env python3
from Decontaminate import Decontaminate
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    decontaminator = Decontaminate(*args, **kwargs)
    return(decontaminator.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
