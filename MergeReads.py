#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class MergeReads(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(MergeReads, self).__init__(*args, **kwargs)

    def make_command(self, read):
        mate = self.mate(read)
        out = self.replace_read_marker_with("_pe", read)
        out = self.rebase_file(out)
        outu = self.replace_extension_with(".unmerged.fq.gz", out)
        command = ("bbmerge.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} out={o} "
                   "outu={ou} iterations=5 extend2=20 ecct usejni=t").format(
            xmx=self.get_mem(fraction=0.95),
            t=self.get_threads(),
            i1=read,
            i2=mate,
            o=out,
            ou=outu
        )  # K = 31 by default

        return (command)
