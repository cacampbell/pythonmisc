#!/usr/bin/env python
import re

from ParallelCommand import ParallelCommand


class RecalibrateBases(ParallelCommand):
    def __init__(self, input_root, output_root):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(RecalibrateBases, self).__init__(input_root, output_root)

    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        out1 = self.rebase_file(read)
        out2 = self.rebase_file(mate)
        bam = re.sub(self.read_marker, "_pe", read)
        bam = re.sub(self.input_suffix, ".bam", bam)
        command = ("bbduk.sh in1={i1} in2={i2} sam={bam} usejni=t "
                   "out1={o1} out2={o2} recalibrate").format(i1=read,
                                                             i2=mate,
                                                             bam=bam,
                                                             o1=out1,
                                                             o2=out2)
        return command
