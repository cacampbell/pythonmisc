#!/usr/bin/env python3
from sys import stderr

from os import walk
from os.path import basename
from os.path import normpath
from re import search

from PairedEndCommand import PairedEndCommand


class ReadGrouper(PairedEndCommand):
    """
    Add Basic Read Group information to reads in order to pass VALIDATION=STRICT
    with GATK tools. Read group information will reflect lowermost directory
    strucutre -- assuming that each lowermost directory below input_root is a
    different library.
    """

    def __init__(self, *args, **kwargs):
        super(ReadGrouper, self).__init__(*args, **kwargs)
        self.set_default("tmp_dir", "~/tmp")
        self.set_default("picard", "picard.jar")
        self.set_default("platform", "illumina")
        self.set_default("barcode_file", "barcodes.txt")

    def make_command(self, filename):
        pass

    def read_barcode_file(self):
        d = {}

        with open(self.barcode_file, "r") as fh:
            for line in fh.readlines():
                key, val = line.split()
                d[key] = val

        return d.items()

    def get_platform_unit(self, filename):
        # Extract the barcode from the filename, and get the Lane number
        lane = 1
        barcode = ""

        try:
            lane = int(search("(?<=_L)[0-9].*?(?=_pe)", filename).group(0))
        except AttributeError:
            if self.verbose:
                print("Could not determine lane number", file=stderr)

        for key, val in self.read_barcode_file():
            if key in filename:
                barcode = val

        return "{}.{}".format(barcode, lane)

    def format_commands(self):
        # Assume <sample>_[<barcode>_]L<lane number>_R<read_number>_<set>.<ext>
        # So the samples are the first chunk of the basename
        def __sample(filename):
            if search("_L[0-9]{3}", filename):
                return filename.split("_L")[0]
            elif search("_R[1|2]", filename):
                return filename.split("_R")[0]
            else:
                return filename

        libraries = {}
        sample_files = {}
        libnames = [x for x, dirs, files in walk(self.input_root) if not dirs]
        samples = list(set([__sample(basename(x)) for x in self.files]))

        # For each sample, gather files that belong to that sample
        for sample in samples:
            sample_files[sample] = [x for x in self.files if sample in x]

        for lib in libnames:
            libraries[normpath(basename(lib))] = {
                key: val for (key, val) in sample_files.items() if
                all([lib in x for x in val])
                }

        if self.verbose:
            print("Library Structure:", file=stderr)
            for lib, samples in libraries.items():
                print(lib, file=stderr)
                print(samples, file=stderr)

        for lib, samples in libraries.items():
            for sample, files in samples.items():
                for filename in files:
                    output = self.rebase_file(filename)
                    j = "{}{}".format(self.cluster_options["job_name"],
                                      basename(filename))
                    command_str = ("java -Xms{xms} -Xmx{xmx} "
                                   "-Djava.io.tmpdir={tmpdir} -jar "
                                   "{picard} AddOrReplaceReadGroups I={i} "
                                   "O={o} RGLB={libname} RGPL={platform} "
                                   "RGPU={unit} RGSM={sample}").format(
                        xmx=self.get_mem(fraction=0.98),
                        xms=self.get_mem(fraction=0.95),
                        tmpdir=self.tmp_dir,
                        picard=self.picard,
                        i=filename,
                        o=output,
                        libname=lib,
                        platform=self.platform,
                        unit=self.get_platform_unit(filename),
                        sample=sample)

                    self.commands[j] = command_str

                    if self.verbose:
                        print(command_str, file=stderr)

    def run(self):
        if self.verbose:
            print("Loading environment modules...", file=stderr)
            if self.modules is not None:
                self.module_cmd(['load'])

        if self.verbose:
            print("Gathering input files...", file=stderr)
        self.get_files()

        if self.verbose:
            print("Removing Exclusions...", file=stderr)

        if self.verbose:
            print("Making output directories...", file=stderr)
        self.make_directories()

        if self.exclusions_paths:
            self.exclude_files_below(self.exclusions_paths)

        self.exclude_files_below(self.output_root)

        if self.exclusions:
            self.remove_regex_from_input(self.exclusions)

        if self.verbose:
            print("Formatting commands...", file=stderr)
        self.format_commands()

        if self.verbose:
            print("Dispatching to cluster...", file=stderr)
        jobs = self.dispatch()

        return(jobs)
