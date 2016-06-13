#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class Decontaminate(PairedEndCommand):
    def __init__(*args, **kwargs):
        super(Decontaminate, self).__init__(*args, **kwargs)
        self.set_default("reference",
                         "/home/cacampbe/.prog/bbmap/resources/"
                         "phix_adapters.fa.gz")

    def make_command(self, read):
        mate = self.mate(read)
        out1 = self.rebase_file(read)
        out2 = self.rebase_file(mate)
        outm1 = self.replace_extension(".contam.fq", read)
        outm1 = self.rebase_file(outm1)
        outm2 = self.replace_extension(".contam.fq", mate)
        outm2 = self.rebase_file(outm2)
        contam = self.reference
        stats = self.replace_read_marker_with("_stats")
        stats = self.replace_extension(".txt", stats)
        stats = self.rebase_file(stats)
        command = ("bbduk.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} "
                   "out1={o1} out2={o2} outm1={om1} outm2={om2} ref={contam} "
                   "k=31 hdist=1 stats={stats}".format(
                       xmx=self.get_mem(fraction=0.95),
                       threads=self.get_threads(),
                       i1=read,
                       i2=mate,
                       o1=out1,
                       o2=out2,
                       om1=outm1,
                       om2=outm2,
                       ref=contam,
                       stats=stats
                   ))

