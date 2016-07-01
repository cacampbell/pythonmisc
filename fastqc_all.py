#!/usr/bin/env python3
from FastqcAll import FastqcAll
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    fqc = FastqcAll(*args, **kwargs)
    return (fqc.run())

if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
