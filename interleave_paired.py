#!/usr/bin/env python3
from InterleavePaired import InterleavePaired
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    interleave = InterleavePaired(*args, **kwargs)
    return (interleave.run())


if __name__ == "__main__":
    run_parallel_command_with_args(main)
