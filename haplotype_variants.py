#!/usr/bin/env python3
from HaplotypeCaller import HaplotypeCaller
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    hc = HaplotypeCaller(*args, **kwargs)
    return (hc.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
