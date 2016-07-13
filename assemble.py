#!/usr/bin/env python3
# from TadpoleAssemble import TadpoleAssemble
from TrinityAssemble import TrinityAssemble
from parallel_command_parse import run_parallel_command_with_args


def main(*args, **kwargs):
    if "assembler" in kwargs.keys():
        if kwargs["assembler"].upper().strip() == "TADPOLE":
            #assembler = TadpoleAssemble(*args, **kwargs)
            #assembler.input_regex = ".*"
            #assembler.read_regex = ".*"
            ## assembler.extension = r".fq.gz"
            #assembler.modules = ['java/1.8', 'slurm']
            #jobs = assembler.run()
            #return (jobs)
            return None
        elif kwargs["assembler"].upper().strip() == "TRINITY":
            assembler = TrinityAssemble(*args, **kwargs)
            assembler.input_regex = ".*"
            assembler.read_regex = ".*"
            if "genome_guided" in kwargs.keys():
                assembler.extension = ".bam"
                assembler.modules = ["java", "samtools/1.3.1", "trinity"]
                jobs = assembler.run()
                return (jobs)
            else:
                assembler.extension = ".fq"
                assembler.modules = ["java", "trinity"]
                jobs = assembler.run()
                return (jobs)


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
