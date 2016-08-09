#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class FileMerger(PairedEndCommand):
    """
    Merge files that were split using the "split_files" command, such that each
    file that was output to a split.XXXX file is combined into a single file,
    based on the final extension of the file. I.e. input/file.split.XXXX.fq
    will be merged into output/file.fq. This handles fq, sam, and bam files.

    Also included is the option to merge files by sample name, where the sample
    name is assumed to be the prefix of the basename of the file (as with
    illumina sequencing reads). This process will not combine files from
    separate directories below the input root.
    """

    def __init__(self, *args, **kwargs):
        super(FileMerger, self).__init__(*args, **kwargs)
        self.set_default("by_sample", False)

    def make_command(self, filename):
        pass

    def format_commands(self):
        pass

    def run(self):
        pass
