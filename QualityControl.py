#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class QualityControl(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(QualityControl, self).__init__(*args, **kwargs)
        self.set_default("reference",
                         "/home/cacampbe/.prog/bbmap/resources/adapters.fa")

    def make_command(self, read):
        mate = self.mate(read)
        output1 = self.rebase_file(read)
        output2 = self.rebase_file(mate)
        command = ("bbduk2.sh -Xmx{maxheap} threads={t} in1={i1} in2={i2} "
                   "out1={o1} out2={o2} ref={adpt} ktrim=r "
                   "k=25 mink=11 hdist=2 tpe tbo qtrim=rl trimq=10 ").format(
            maxheap=self.get_mem(fraction=0.95), i1=read, i2=mate, o1=output1,
            o2=output2, adpt=self.reference, t=self.get_threads())
        return command
