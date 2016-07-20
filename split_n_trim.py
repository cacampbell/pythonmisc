#!/usr/bin/env python3
from SplitNTrim import SplitNTrim
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    splitntrim = SplitNTrim(*args, **kwargs)
    return(splitntrim.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
