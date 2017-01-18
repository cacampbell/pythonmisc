#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class Reformat(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Reformat, self).__init__(*args, **kwargs)
        self.set_default("input_regex", ".*")
        self.set_default("read_regex", "_R1")
        self.set_default("extension", ".fq.gz$")
        self.set_default("operation", "")
        self.set_default("qin", "64")
        self.set_default("ignorebadquality", False)

    def make_command(self, read):
        if self.operation == "":
            if ".fq" in self.extension:
                self.operation = "qout33"
            elif ".sam" in self.extension or ".bam" in self.extension:
                self.operation = "sam1.3"

        if self.operation in ["qout33", "qout64"]:
            mate = self.mate(read)
            out_read = self.rebase_file(read)
            out_mate = self.rebase_file(mate)
            qual = self.operation[-2:]
            inqual = self.qin
            command = ("reformat.sh -Xmx{xmx} in={in1} "
                       "in2={in2} out1={out1} "
                       "out2={out2} qin={qin} qout={qout}").format(
                           xmx=self.get_mem(0.99),
                           in1=read,
                           in2=mate,
                           out1=out_read,
                           out2=out_mate,
                           qin=inqual,
                           qout=qual
                       )
            if self.ignorebadquality:
                command += " ignorebadquality"

            return(command)
        elif self.operation in ["sam1.3"]:
            outf = self.rebase_file(read)
            command = ("reformat.sh -Xmx{xmx} in={i} "
                       "out={o} sam=1.3").format(xmx=self.get_mem(0.99),
                                                 i=read,
                                                 o=outf)
            return(command)
