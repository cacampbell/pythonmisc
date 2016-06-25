#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class Repair(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Repair, self).__init__(*args, **kwargs)

    def make_command(self, read):
        mate = self.mate(read)
        output1 = self.replace_read_marker_with("_interleaves", read)
        output1 = self.rebase_file(output1)
        output2 = self.replace_read_marker_with("_interleaves", mate)
        output2 = self.rebase_file(output2)
        command = ("repair.sh in1={r} in2={m} out1={o1} out2={o2} "
                   "-Xmx{xmx} t={t}").format(r=read,
                                             m=mate,
                                             o1=output1,
                                             o2=output2,
                                             xmx=self.get_mem(fraction=0.95),
                                             t=self.get_threads())
        return (command)
