#!/usr/bin/env python3

from sys import stderr

from os.path import basename
from os.path import join

from PairedEndCommand import PairedEndCommand
from ParallelCommand import mkdir_p
from combiner import combine_files


class TadpoleAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(TadpoleAssemble, self).__init__(*args, **kwargs)
        self.set_default("all_reads_name", "all_reads.fq")
        self.set_default("assembly_name", "contigs.fa")
        self.set_default("mincontig", "250")
        self.set_default("mincov", "3")
        self.set_default("kmer_len", "31")

    def make_command(self, read):
        pass

    def format_commands(self):
        input_f = join(self.input_root, self.all_reads_name)
        output = join(self.output_root, self.assembly_name)

        if self.verbose:
            print("Combining input files...", file=stderr)

        if not self.dry_run:
            combine_files(self.files, input_f)
        else:
            print("Would have combined files if not dry_run...", file=stderr)

        job_name = "{}{}".format(self.cluster_options["job_name"],
                                 basename(self.all_reads_name))
        command = ("tadpole.sh in={} out={} prealloc=t prefilter=2 "
                   "mincontig={} mincov={} k={}").format(input_f,
                                                         output,
                                                         self.mincontig,
                                                         self.mincov,
                                                         self.kmer_len)
        self.commands[job_name] = command

        if self.verbose:
            print(command, file=stderr)

    def run(self):
        """
            Run the Parallel Command from start to finish
            1) Load Environment Modules
            2) Gather input files
            3) Remove exclusions
            4) Make Directories
            5) Format Commands
            6) Dispatch Scripts to Cluster Scheduler
            7) Unload the modules
            :return: list<str>: a list of job IDs returned by cluster scheduler
            """
        if self.verbose:
            print('Loading environment modules...', file=stderr)
            if self.modules is not None:
                self.module_cmd(['load'])

        if self.verbose:
            print('Gathering input files...', file=stderr)
        self.get_files()

        if self.verbose:
            print('Removing exclusions...', file=stderr)

        if self.verbose:
            print("Making output directories...", file=stderr)
        mkdir_p(self.output_root)

        if self.exclusions_paths:
            self.exclude_files_below(self.exclusions_paths)

        self.exclude_files_below(self.output_root)

        if self.exclusions:
            self.remove_regex_from_input(self.exclusions)

        self.remove_regex_from_input(self.all_reads_name)

        if self.verbose:
            print('Formatting commands...', file=stderr)
        self.format_commands()

        if self.verbose:
            print('Dispatching to cluster...', file=stderr)
        jobs = self.dispatch()  # Return the job IDs from the dispatched cmds

        return (jobs)
