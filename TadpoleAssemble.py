#!/usr/bin/env python3
from sys import stderr

from os.path import basename

from PairedEndCommand import PairedEndCommand
from combiner import combine_files


class TadpoleAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(TadpoleAssemble, self).__init__(*args, **kwargs)
        self.set_default("all_reads_name", "all_reads.fq.gz")
        self.set_default("assembly_name", "contigs.fa")
        self.set_default("mincontig", "250")
        self.set_default("mincov", "3")

    def make_command(self, read):
        pass

    def format_commands(self):
        combine_files(self.files, self.all_reads_name)
        job_name = "{}{}".format(self.cluster_options["job_name"],
                                 basename(self.all_reads_name))
        command = ("tadpole.sh in={} out={} careful prealloc=t prefilter=2 "
                   "mincontig={} mincov={}").format(self.all_reads_name,
                                                    self.assembly_name,
                                                    self.mincontig,
                                                    self.mincov)
        self.commands[job_name] = command

        if self.verbose:
            print(command, file=stderr)
