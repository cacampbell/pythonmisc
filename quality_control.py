#!/usr/bin/env python3
from QualityControl import QualityControl
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    qc = QualityControl(*args, **kwargs)
    qc.modules = ['java', 'slurm']
    return (qc.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
