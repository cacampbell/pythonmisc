#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class InterleavePaired(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(InterleavePaired, self).__init__(*args, **kwargs)

    def make_command(self, read):
        mate = self.mate(read)
        output = self.replace_read_marker_with("_interleaves", read)
        output = self.rebase_file(output)
        command = ("reformat.sh in1={r} in2={m} out={o} "
                   "-Xmx{xmx} t={t}").format(r=read,
                                             m=mate,
                                             o=output,
                                             xmx=self.get_mem(fraction=0.95),
                                             t=self.get_threads())
        return (command)
