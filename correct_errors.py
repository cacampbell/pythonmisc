#!/usr/bin/env python3
from ErrorCorrect import ErrorCorrect
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    ec = ErrorCorrect(*args, **kwargs)
    # Also takes --stats, --normalize, --min_depth=, --target_depth=
    # Defaults: False, False, 6, 40
    return (ec.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
