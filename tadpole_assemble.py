#!/usr/bin/env python3
from TadpoleAssemble import TadpoleAssemble
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    assembler = TadpoleAssemble(*args, **kwargs)
    assembler.input_regex = ".*"
    assembler.read_regex = ".*"
    # assembler.extension = r".fq.gz"
    assembler.modules = ['java/1.8', 'slurm']
    jobs = assembler.run()
    return(jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
