#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re
import os


class FastqcAll(ParallelCommand):
    def __init__(self, input_, output_):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(FastqcAll, self).__init__(input_, output_)

    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        out = self.output_file(read)
        outdir = os.path.dirname(out)
        command = "fastqc {r} {m} --outdir={o}".format(r=read, m=mate, o=outdir)
        return command
