#!/usr/bin/env python3
from GenotypeCVCFs import GenotypeGVCFs
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    gg = GenotypeGVCFs(*args, **kwargs)
    return (gg.run())


if __name__ == "__main__":
    run_parallel_command_with_args(main)
