#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class RemoveHuman(ParallelCommand):
    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        output1 = self.output_file(read)
        output2 = self.output_file(mate)
        human1 = re.sub(self.read_marker, "_Human1", output1)
        human2 = re.sub(self.mate_marker, "_Human2", output2)
        stats_f = re.sub(self.read_marker, "_Stats", output1)
        stats_f = re.sub(self.input_suffix, ".stats.txt", stats_f)
        command = ("bbmap.sh -Xmx{maxh}G "
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
                                                  h1=human1,
                                                  h2=human2,
                                                  s=stats_f,
                                                  t=self.get_threads())

        return command
