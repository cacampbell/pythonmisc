#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re
import os
import sys


class BBMapper(ParallelCommand):
    def __init__(self, input_, output_):
        self.exclusions = "exclusions.txt"
        super(BBMapper, self).__init__()

    def get_exclusions(self):
        """
        Read the file at self.exclusions, or continue if that file does not
        exist (use ['$^'] as exclusions)
        """
        exclusions = []

        if not os.path.isfile(self.exlusions):
            return ['$^']
        else:
            try:
                with open(self.exclusions, 'r+') as fh:
                    lines = fh.readlines()
                    for line in lines:
                        exclusions += [line]
            except OSError as e:
                print(("Error reading exclusions file, "
                       "using no exclusions instead: {}").format(e))
                exclusions = ['$^']
            finally:
                return exclusions

    def get_files(self):
        """
        Gather files below the input_root such that those files end with
        input_suffix and contain read_marker within their name. For example,
        input_suffix might be ".fq.gz" and read_marker might be "R1". This
        method would thus gather all reads that match ^.*R1.*\.fq\.gz$

        Exclude all files that have a regex match with the strings listed in
        self.exclusions (one per line). If there is no file listed at
        self.exclusions, then a '$^' will be used (this doesn't match anything
        by definition)
        :return:
        """
        exclusions = self.get_exclusions()
        for root, _, files in os.walk(self.input_root):
            for filename in files:
                if filename.endswith(self.input_suffix):  # input suffix
                    for exclusion in exclusions:
                        if not re.search(exclusion, filename):
                            if self.read_marker in filename:  # read marker
                                abs_path = os.path.join(root, filename)
                                self.__files += [abs_path]

                                if self.verbose:
                                    print(abs_path, file=sys.stderr)

    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        map_sam = re.sub(self.read_marker, "_pe", read)
        map_sam = re.sub(self.input_suffix, ".sam", map_sam)
        map_sam = self.output_file(map_sam)
        unmap_sam = re.sub(".sam", ".unmapped.sam", map_sam)
        covstat = re.sub(self.read_marker, "_covstats", mate)
        covstat = re.sub(self.input_suffix, ".txt", covstat)
        covstat = self.output_file(covstat)
        covhist = re.sub(self.read_marker, "_covhist", read)
        covhist = re.sub(self.input_suffix, ".txt", covhist)
        covhist = self.output_file(covhist)
        basecov = re.sub(self.read_marker, "_basecov", read)
        basecov = re.sub(self.input_suffix, ".txt", basecov)
        basecov = self.output_file(basecov)
        bincov = re.sub(self.read_marker, "_bincov", read)
        bincov = re.sub(self.input_suffix, ".txt", bincov)
        bincov = self.output_file(bincov)
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} ref={r} "
                   "nodisk covstats={covstat} covhist={covhist} threads={t} "
                   "slow k=12 -Xmx{xmx} basecov={basecov} usejni=t"
                   " bincov={bincov}").format(i1=read,
                                              i2=mate,
                                              om=map_sam,
                                              ou=unmap_sam,
                                              r=self.reference,
                                              covstat=covstat,
                                              covhist=covhist,
                                              basecov=basecov,
                                              bincov=bincov,
                                              xmx=self.get_mem(),
                                              t=self.get_threads())
        return command
