#!/usr/bin/env python
import os
import re

from ParallelCommand import ParallelCommand


class FastqcAll(ParallelCommand):
    def __init__(self, input_root, output_root):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(FastqcAll, self).__init__(input_root, output_root)

    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        out = self.rebase_file(read)
        outdir = os.path.dirname(out)
        command = "fastqc {r} {m} --outdir={o}".format(r=read, m=mate, o=outdir)
        return command
