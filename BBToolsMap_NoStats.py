#!/usr/bin/env python
from __future__ import print_function

from PairedEndCommand import PairedEndCommand


class BBMapperNoStats(PairedEndCommand):
    def __init__(self, input_root, output_root, input_regex=".fq.gz"):
        PairedEndCommand.__init__(input_root=input_root,
                                  output_root=output_root)
        self.input_regex = input_regex

    def make_command(self, read):
        # Mate File
        mate = self.mate(read)

        # Mapped Sam file
        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)

        # Unmapped Sam file
        unmap_sam = self.replace_extension(".unmapped.sam", read)

        # Full Command
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} nodisk "
                   "threads={t} ref={r} slow k=12 -Xmx{xmx} basecov={basecov} "
                   "usejni=t").format(i1=read,
                                      i2=mate,
                                      om=map_sam,
                                      ou=unmap_sam,
                                      xmx=self.get_mem(),
                                      t=self.get_threads(),
                                      r=self.reference)
        return (command)
