#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re


class BBMapper(ParallelCommand):
    def __init__(self, input_, output_):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(BBMapper, self).__init__(input_, output_)

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
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou}"
                   "covstats={covstat} covhist={covhist} threads={t} "
                   "slow k=12 -Xmx{xmx} basecov={basecov} usejni=t"
                   " bincov={bincov}").format(i1=read,
                                              i2=mate,
                                              om=map_sam,
                                              ou=unmap_sam,
                                              covstat=covstat,
                                              covhist=covhist,
                                              basecov=basecov,
                                              bincov=bincov,
                                              xmx=self.get_mem(),
                                              t=self.get_threads())
        return command
