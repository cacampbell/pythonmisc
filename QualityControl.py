#!/usr/bin/env python
import re

from ParallelCommand import ParallelCommand


class QualityControl(ParallelCommand):
    def __init__(self, input_root, output_root):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(QualityControl, self).__init__(input_root, output_root)

    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        output1 = self.rebase_file(read)
        output2 = self.rebase_file(mate)
        command = ("bbduk2.sh -Xmx{maxheap} threads={t} in1={i1} in2={i2} "
                   "out1={o1} out2={o2} ref={adpt} ktrim=r "
                   "k=25 mink=11 hdist=2 tpe tbo qtrim=rl trimq=10 ").format(
            maxheap=self.get_mem(), i1=read, i2=mate, o1=output1,
            o2=output2, adpt=self.reference, t=self.get_threads())
        return command
