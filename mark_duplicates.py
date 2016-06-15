from MarkDuplicates import MarkDuplicates
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    marker = MarkDuplicates(*args, **kwargs)
    return (marker.run())

if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
