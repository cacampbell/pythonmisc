#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class QualityControl(ParallelCommand):
    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        output1 = self.output_file(read)
        output2 = self.output_file(read)
        command = ("bbduk2.sh -Xmx{maxheap}G in1={i1} in2={i2} out1={o1} "
                   "out2={o2} ref={adpt} qtrim=rl trimq=10 ktrim=r k=21 "
                   "mink=11 hdist=2 maq=10 mlf=50 tpe tbo threads={t}").format(
            maxheap=self.get_mem(), i1=read, i2=mate, o1=output1,
            o2=output2, adpt=self.reference, t=self.get_threads())
        return command