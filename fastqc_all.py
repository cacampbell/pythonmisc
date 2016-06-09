#!/usr/bin/env python
from FastqcAll import FastqcAll
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    fqc = FastqcAll(*args, **kwargs)
    return (fqc.run())

if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
