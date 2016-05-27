#!/usr/bin/env python
from BBToolsMap import BBMapper
from BBToolsMap_NoStats import BBMapperNoStats
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    mapper = BBMapperNoStats(*args, **kwargs)

    if 'stats' in kwargs:
        if kwargs['stats'] is True:
            mapper = BBMapper(*args, **kwargs)

    return (mapper.run())  # List of slurm jobs generated


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
