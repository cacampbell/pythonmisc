#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re
import os


class FastqcAll(ParallelCommand):
    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        out = self.output_file(read)
        outdir = os.path.dirname(out)
        command = "fastqc {r} {m} --outdir={o}".format(r=read, m=mate, o=outdir)
        return command
