#!/usr/bin/env python
from AdapterSearch import AdapterFinder
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    adapter_finder = AdapterFinder(*args, **kwargs)
    return (adapter_finder.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
