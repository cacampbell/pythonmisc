#!/usr/bin/env python
from __future__ import print_function

from PairedEndCommand import PairedEndCommand


class BBMapper(PairedEndCommand):
    def __init__(self, input_root, output_root, input_regex=".fq.gz"):
        super(BBMapper, self).__init__(input_root=input_root,
                                       output_root=output_root,
                                       input_regex=input_regex)

    def make_command(self, read):
        # Mate File
        mate = self.mate(read)

        # Mapped Sam file
        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)

        # Unmapped Sam file
        unmap_sam = self.replace_read_marker_with("_pe", read)
        unmap_sam = self.replace_extension(".unmapped.sam", unmap_sam)
        unmap_sam = self.rebase_file(unmap_sam)

        # Coverage Statistics
        covstat = self.replace_read_marker_with("_covstats", read)
        covstat = self.replace_extension(".txt", covstat)
        covstat = self.rebase_file(covstat)

        # Coverage Hist
        covhist = self.replace_read_marker_with("_covhist", read)
        covhist = self.replace_extension(".txt", covhist)
        covhist = self.rebase_file(covhist)

        # Base Coverage
        basecov = self.replace_read_marker_with("_basecov", read)
        basecov = self.replace_extension(".txt", basecov)
        basecov = self.rebase_file(basecov)

        # Bin Coverage
        bincov = self.replace_read_marker_with("_bincov", read)
        bincov = self.replace_extension(".txt", bincov)
        bincov = self.rebase_file(bincov)

        # Full Command
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} nodisk "
                   "covstats={covstat} covhist={covhist} threads={t} ref={r} "
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
                                              t=self.get_threads(),
                                              r=self.reference)
        return (command)
