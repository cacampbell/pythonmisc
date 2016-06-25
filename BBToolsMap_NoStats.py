#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class BBMapperNoStats(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(BBMapperNoStats, self).__init__(*args, **kwargs)
        self.set_default("reference", "reference.fa")
        # Set read_regex here if necessary

    def make_command(self, read):
        mate = self.mate(read)
        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension_with(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)
        unmap_sam = self.replace_read_marker_with("_pe", read)
        unmap_sam = self.replace_extension_with(".unmapped.sam", unmap_sam)
        unmap_sam = self.rebase_file(unmap_sam)
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} nodisk "
                   "threads={t} ref={r} slow k=12 -Xmx{xmx}  "
                   "usejni=t").format(i1=read,
                                      i2=mate,
                                      om=map_sam,
                                      ou=unmap_sam,
                                      xmx=self.get_mem(),
                                      t=self.get_threads(),
                                      r=self.reference)
        return (command)
