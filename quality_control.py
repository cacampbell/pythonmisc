#!/usr/bin/env python
from QualityControl import QualityControl
from simple_argparse import run_parallel_command_with_args


def main(*args, **kwargs):
    qc = QualityControl(*args, **kwargs)
    qc.run()


if __name__ == "__main__":
    run_parallel_command_with_args(main)
