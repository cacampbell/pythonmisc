#!/usr/bin/env python3
from sys import stderr

from TadpoleAssemble import TadpoleAssemble
from TrinityAssemble import TrinityAssemble
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    if "assembler" in kwargs.keys():
        if kwargs["assembler"].upper().strip() == "TADPOLE":
            assembler = TadpoleAssemble(*args, **kwargs)
            assembler.input_regex = ".*"
            assembler.read_regex = ".*"
            # assembler.extension = r".fq.gz"
            assembler.modules = ['java/1.8', 'slurm']
            jobs = assembler.run()
            return (jobs)
        elif kwargs["assembler"].upper().strip() == "TRINITY":
            assembler = TrinityAssemble(*args, **kwargs)
            assembler.input_regex = ".*"
            assembler.read_regex = ".*"
            if "genome_guided" in kwargs.keys():
                assembler.extension = ".bam"
                assembler.modules = ["java/1.8", "samtools/1.3.1", "slurm"]
                jobs = assembler.run()
                return (jobs)
            else:
                assembler.extension = ".fq"
                assembler.modules = ["java/1.8", "slurm"]
                jobs = assembler.run()
                return (jobs)
    else:
        print("Please specify an assembler", file=stderr)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
