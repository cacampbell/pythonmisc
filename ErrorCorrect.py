#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class ErrorCorrect(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(ErrorCorrect, self).__init__(*args, **kwargs)
        self.set_default("normalize", False)
        self.set_default("stats", False)
        self.set_default("min_depth", "6")
        self.set_default("target_depth", "40")

    def make_command(self, read):
        mate = self.mate(read)
        out1 = self.rebase_file(read)
        out2 = self.rebase_file(mate)

        if self.normalize:
            command = ("bbnorm.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} "
                       "out1={o1} out2={o2} mindepth={md} target={d} ecc=t "
                       "mue=t").format(
                xmx=self.get_mem(fraction=0.95),
                t=self.get_threads(),
                i1=read,
                i2=mate,
                o1=out1,
                o2=out2,
                md=self.min_depth,
                d=self.target_depth
            )

            if self.stats:
                hist = self.replace_extension_with(".txt", read)
                hist = self.replace_read_marker_with("_pe", hist)
                hist = self.rebase_file(hist)
                command = ("{cmd} hist={h}").format(cmd=command, h=hist)

            return (command)
        else:
            command = ("ecc.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} "
                       "out1={o1} out2={o2} mue=t").format(
                xmx=self.get_mem(fraction=0.95),
                t=self.get_threads(),
                i1=read,
                i2=mate,
                o1=out1,
                o2=out2
            )

            if self.stats:
                hist = self.replace_extension_with(".txt", read)
                hist = self.replace_read_marker_with("_pe", hist)
                hist = self.rebase_file(hist)
                command = ("{cmd} hist={h}").format(cmd=command, h=hist)

            return (command)
