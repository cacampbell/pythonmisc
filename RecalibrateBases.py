#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class RecalibrateBases(ParallelCommand):
    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        out1 = self.output_file(read)
        out2 = self.output_file(mate)
        bam = re.sub(self.read_marker, "_pe", read)
        bam = re.sub(self.input_suffix, ".bam", bam)
        command = ("bbduk.sh in1={i1} in2={i2} sam={bam} usejni=t "
                   "out1={o1} out2={o2} recalibrate").format(i1=read,
                                                             i2=mate,
                                                             bam=bam,
                                                             o1=out1,
                                                             o2=out2)
        return command
