#!/usr/bin/env python3
from MergeFiles import FileMerger
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    f = FileMerger(*args, **kwargs)
    f.run()


if __name__ == "__main__":
    run_parallel_command_with_args(main)
