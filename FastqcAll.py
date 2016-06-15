#!/usr/bin/env python
import os

from PairedEndCommand import PairedEndCommand


class FastqcAll(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(FastqcAll, self).__init__(*args, **kwargs)

    def make_command(self, read):
        mate = self.mate(read)
        out = self.rebase_file(read)
        outdir = os.path.dirname(out)
        command = "fastqc {r} {m} --outdir={o}".format(r=read, m=mate, o=outdir)
        return (command)
