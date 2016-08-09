#!/usr/bin/env python3
from VariantCaller import VariantCaller
from parse_parallel_command import run_parallel_command_with_args


def main(*args, **kwargs):
    vc = VariantCaller(*args, **kwargs)
    vc.modules = ["java"]
    jobs = vc.run()
    return(jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
