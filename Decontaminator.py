#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class Decontaminator(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Decontaminator, self).__init__(self, *args, **kwargs)
        self.set_default("reference",
                         "/home/cacampbe/.prog/bbmap/resources/"
                         "phix_adapters.fa.gz,/home/cacampbe/.prog/bbmap/"
                         "resources/phix174_ill.ref.fa.gz")

    def make_command(self, read):
        mate = self.mate(read)
        out1 = self.rebase_file(read)
        out2 = self.rebase_file(mate)
        outm1 = self.replace_extension_with(".contam.fq.gz", read)
        outm1 = self.rebase_file(outm1)
        outm2 = self.replace_extension_with(".contam.fq.gz", mate)
        outm2 = self.rebase_file(outm2)
        stats = self.replace_read_marker_with("_stats", read)
        stats = self.replace_extension_with(".txt", stats)
        stats = self.rebase_file(stats)
        command = ("bbduk.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} out1={o1} "
                   "out2={o2} outm1={m1} outm2={m2} stats={s} ref={r} k=31 "
                   "hdist=1").format(
            xmx=self.get_mem(fraction=0.95), t=self.get_threads(), i1=read,
            i2=mate, o1=out1, o2=out2, m1=outm1, m2=outm2, r=self.reference,
            s=stats
        )
        return (command)
