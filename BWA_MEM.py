#!/usr/bin/env python
from Algorithm_Options import Algorithm_Options
# from clusterlib.scheduler import submit
# from clusterlib.scheduler import queued_or_running_jobs
from IO_Options import IO_Options
import os
# import re
from Scoring_Options import Scoring_Options
# from subprocess import call
from Slurm_Options import Slurm_Options
import sys
# from uuid import uuid4
import unittest

# bwa mem  [-aCHMpP]  [-t  nThreads] [-k minSeedLen] [-w bandWidth]
# [-d zDropoff] [-r seedSplitRatio] [-c maxOcc] [-A match Score] [-B mmPenalty]
# [-O gapOpenPen] [-E gapExtPen] [-L clipPen] [-U unpairPen] [-R RGline]
# [-v  verboseLevel]  db.prefix reads.fq [mates.fq]


class BWA_MEM:
    def __init__(self, genome_prefix, root_directory, read_type, verbose,
                 algorithm_options, scoring_options, io_options, slurm_options):
        self.genome_prefix = genome_prefix
        self.root_directory = root_directory
        self.read_type = read_type
        self.verbose = verbose
        self.algorithm_options = algorithm_options
        self.scoring_options = scoring_options
        self.io_options = io_options
        self.slurm_options = slurm_options

    def check_parameters(self):
        if self.verbose:
            sys.stdout.write("Validating parameters...\n")

        assert(os.path.isdir(os.path.dirname(self.genome_prefix)))
        assert(os.path.isdir(self.root_directory))
        assert(self.read_type in ["single end", "paired end", "interleaved"])
        assert(isinstance(self.verbose, bool))
        assert(isinstance(self.algorithm_options, Algorithm_Options))
        assert(isinstance(self.scoring_options, Scoring_Options))
        assert(isinstance(self.io_options, IO_Options))
        assert(isinstance(self.slurm_options, Slurm_Options))
        self.algorithm_options.validate(self.verbose)
        self.scoring_options.validate(self.verbose)
        self.io_options.validate(self.verbose)
        self.slurm_options.validate(self.verbose)

    def get_runs(self):
        runfiles = []
        for root, directories, filenames in os.walk(self.directory):
            for filename in filenames:
                if filename.endswith([".fq", ".fq.gz", ".fastq", ".fastq.gz"]):
                    full_path = os.path.join(root, filename)
                    runfiles.append(full_path)

                    if self.verbose:
                        sys.stdout.write(full_path)

        return list(set(runfiles))

    def __paired_end_commands(self, files):
        commands = []

        return commands

    def __single_end_commands(self, files):
        commands = []

        return commands

    def __interleaved_commands(self, files):
        commands = []

        return commands

    def assemble_commands(self, files):
        if self.read_type == "paired end":
            return self.__paired_end_commands(files)
        elif self.read_type == "single end":
            return self.__single_end_commands(files)
        elif self.read_type == "interleaved":
            return self.__interleaved_commands(files)
        else:
            raise ValueError("BWA_MEM.read_type is not 'paired end', \
                             'single end', or 'interleaved'")

    def assemble_scripts(self, commands):
        scripts = []

        return scripts

    def dispatch_to_slurm(self, scripts):
        pass

    def run(self):
        self.check_parameters()
        run_files = self.get_runs()
        cmds = self.assemble_commands(run_files)
        scripts = self.assemble_scripts(cmds)
        self.dispatch(scripts)


class Test_BWA_MEM(unittest.TestCase):
    def test_get_runs(self, directory):
        pass

    def test_assemble_commands(self):
        pass

    def test_assemble_scripts(self):
        pass

    def test_dispatch_to_slurm(self):
        pass

    def test_run(self):
        pass
