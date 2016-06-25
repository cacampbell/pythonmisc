#!/usr/bin/env python
from Repair import Repair
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    repair = Repair(*args, **kwargs)
    return (repair.run())


if __name__ == "__main__":
    run_parallel_command_with_args(main)
