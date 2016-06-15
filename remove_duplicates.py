#!/usr/bin/env python3
from Deduplicate import Deduplicate
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    deduplicator = Deduplicate(*args, **kwargs)
    # options --use_picard and --by_mapping determine flow of command
    return (deduplicator.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
