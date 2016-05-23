#!/usr/bin/env python
from __future__ import print_function

from PairedEndCommand import PairedEndCommand


class BBMapperNoStats(PairedEndCommand):
    def __init__(self, input_root, output_root, input_regex="fq.gz$"):
        super(BBMapperNoStats, self).__init__(input_root=input_root,
                                              output_root=output_root,
                                              input_regex=input_regex)
        self.read_regex = "_R1(?=\.fq)"

    def make_command(self, read):
        mate = self.mate(read)
        # Mapped Sam file

        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)

        # Full Command
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} nodisk "
                   "threads={t} ref={r} slow k=12 -Xmx{xmx} "
                   "usejni=t").format(i1=read,
                                      i2=mate,
                                      om=map_sam,
                                      xmx=self.get_mem(),
                                      t=self.get_threads(),
                                      r=self.reference)
        return (command)
