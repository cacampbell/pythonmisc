#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class RepairReads(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(RepairReads, self).__init__(*args, **kwargs)

    def make_command(self, read):
        mate = self.mate(read)
        output1 = self.rebase_file(read)
        output2 = self.rebase_file(mate)
        command = ("repair.sh in1={r} in2={m} out1={o1} out2={o2} "
                   "-Xmx{xmx} t={t}").format(r=read,
                                             m=mate,
                                             o1=output1,
                                             o2=output2,
                                             xmx=self.get_mem(fraction=0.95),
                                             t=self.get_threads())
        return (command)
