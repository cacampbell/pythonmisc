#!/usr/bin/env python3
from os.path import basename
from re import search
from re import sub
from PairedEndCommand import PairedEndCommand
from combiner import combine_files


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
        samples = []

        if self.by_sample:
            samples = list(set([basename(x).split("_")[0] for x in self.files]))

        for sample in samples:
            merge = []
            for filename in self.files:
                if "{}_".format(sample) in filename:
                    merge += [filename]

            output = self.rebase_file(merge[0])
            regex = ".split.[a-z]*"

            try:
                match = search(regex, output).group(0)
                output = sub(match, "", output)
            except AttributeError as err:  # no attribute 'group'
                raise (err)

            combine_files(merge, output)

    def run(self):
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


