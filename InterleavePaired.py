#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class InterleavePaired(ParallelCommand):
    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        output = re.sub(self.read_marker, "_interleaved", read)
        output = self.output_file(output)
        command = ("reformat.sh in1={r} in2={m} out={o} "
                   "-Xmx{xmx} t={t}").format(r=read,
                                             m=mate,
                                             o=output,
                                             xmx=self.get_mem(),
                                             t=self.get_threads())
        return command