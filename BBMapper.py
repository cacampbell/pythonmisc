#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re


class BBMapper(ParallelCommand):
    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        map_bam = re.sub(self.read_marker, "_pe", read)
        map_bam = re.sub(self.input_suffix, ".bam", map_bam)
        map_bam = self.output_file(map_bam)
        unmap_bam = re.sub(".bam", ".unmapped.bam", map_bam)
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
                   "slow k=12 -Xmx{xmx} basecov={basecov}"
                   " bincov={bincov}").format(i1=read,
                                              i2=mate,
                                              om=map_bam,
                                              ou=unmap_bam,
                                              r=self.reference,
                                              covstat=covstat,
                                              covhist=covhist,
                                              basecov=basecov,
                                              bincov=bincov,
                                              xmx=self.get_mem(),
                                              t=self.get_threads())
        return command
