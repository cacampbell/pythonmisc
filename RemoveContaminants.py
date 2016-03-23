#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class RemoveContaminants(ParallelCommand):
    def __init__(self, input_, output_):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(RemoveContaminants, self).__init__(input_, output_)

    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        output1 = self.output_file(read)
        output2 = self.output_file(mate)
        contam1 = re.sub(self.input_suffix, ".contam.fq.gz", output1)
        contam2 = re.sub(self.input_suffix, ".contam.fq.gz", output2)
        stats_f = re.sub(self.read_marker, "_pe", output1)
        stats_f = re.sub(self.input_suffix, ".stats.txt", stats_f)
        command = ("bbmap.sh -Xmx{maxh} "
                   "minid=0.95 maxindel=3 "
                   "bwr=0.16 bw=12 quickmatch "
                   "fast minhits=2 ref={r} nodisk "
                   "in1={i1} in2={i2} outu1={o1} "
                   "outu2={o2} outm1={h1} "
                   "outm2={h2} statsfile={s} "
                   "usejni=t threads={t}").format(maxh=self.get_mem(),
                                                  r=self.reference,
                                                  i1=read,
                                                  i2=mate,
                                                  o1=output1,
                                                  o2=output2,
                                                  h1=contam1,
                                                  h2=contam2,
                                                  s=stats_f,
                                                  t=self.get_threads())

        return command
