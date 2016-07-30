#!/usr/bin/env python3
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

        return d

    def get_platform_unit(self, filename):
        # Extract the barcode from the filename, and get the Lane number
        lane = search("(?<=.*_L)\d.*?(?=_R\d.*)", filename).group(0)
        barcode = ""

        for key, val in self.read_barcode_file():
            if key in filename:
                barcode = val

        return "{}.{}".format(barcode, lane)

    def format_commands(self):
        # Assume <sample>_[<barcode>_]L<lane number>_R<read_number>_<set>.<ext>
        # So the samples are the first chunk of the basename
        libraries = {}
        sample_files = {}
        libnames = [x for x, dirs, files in walk(self.input_root) if not dirs]
        samples = [basename(x).split("_")[0] for x in self.files]

        # For each sample, gather files that belong to that sample
        for sample in samples:
            sample_files[sample] = [x for x in self.files if sample in x]

        for lib in libnames:
            libraries[normpath(basename(lib))] = {
                key: val for key, val in sample_files if
                all([lib in x for x in val])
                }

        for lib, samples in libraries:
            for sample, files in samples:
                for filename in files:
                    output = self.rebase_file(filename)
                    self.commands += [("java -Xms{xms} -Xmx{xmx} "
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
                        sample=sample
                    )]

    def run(self):
        pass
