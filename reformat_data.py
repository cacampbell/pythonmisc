#!/usr/bin/env python3
from Reformat import Reformat
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    reformatter = Reformat(*args, **kwargs)
    jobs = reformatter.run()
    return(jobs)


if __name__ == "__main__":
    print(run_parallel_command_with_args(main))
