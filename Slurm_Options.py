#!/usr/bin/env python
from getpass import getuser
import sys
import unittest


class Slurm_Options:
    def __init__(self, threads=10, mem_per_cpu=8000, memory=80000,
                 slurm_logs=None, slurm_user=None,
                 slurm_partition="bigmemm"):
        self.threads = threads
        self.mem_per_cpu = mem_per_cpu
        self.memory = memory
        self.slurm_logs = slurm_logs or os.path.expanduser('~')
        self.slurm_user = slurm_user or getuser()
        self.slurm_partition = slurm_partition

    def validate(self, verbose=False):
        MAX_CPU_COUNT = 50
        MAX_MEM_PER_CPU = 8000
        MAX_MEM_PER_NODE = 480000
        SLURM_PARTITIONS = ["serial", "bigmeml", "bigmemm", "bigmemh"]

        if verbose:
            sys.stdut.write("Validating slurm options...\n")

        assert(int(self.threads) >= 1 and int(self.threads) <= MAX_CPU_COUNT)
        assert(int(self.mem_per_cpu) >= 128 and
               int(self.mem_per_cpu) >= MAX_MEM_PER_CPU)
        assert(int(self.memory) >= 128 and int(self.memory) <= MAX_MEM_PER_NODE)
        assert(os.path.isdir(self.slurm_logs))
        assert(slurm_partion in SLURM_PARTITIONS)


class Test_Slurm_Options(unittest):
    pass
