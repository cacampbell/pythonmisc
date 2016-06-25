#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class DecontaminateByMapping(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(DecontaminateByMapping, self).__init__(*args, **kwargs)
        self.set_default("reference",
                         "/group/nealedata/databases/Hosa/"
                         "hg19_main_mask_ribo_animal_allplant_allfungus.fa")

    def make_command(self, read):
        mate = self.mate(read)
        output1 = self.rebase_file(read)
        output2 = self.rebase_file(mate)
        contam1 = self.replace_extension_with(".contam.fq.gz", output1)
        contam2 = self.replace_extension_with(".contam.fq.gz", output2)
        stats_f = self.replace_read_marker_with("_stats", read)
        stats_f = self.replace_extension_with(".txt", stats_f)
        stats_f = self.rebase_file(stats_f)
        command = ("bbmap.sh -Xmx{maxh} threads={t} minid=0.95 maxindel=3 "
                   "bwr=0.16 bw=12 quickmatch fast minhits=2 ref={r} nodisk "
                   "in1={i1} in2={i2} outu1={o1} outu2={o2} outm1={h1} "
                   "outm2={h2} statsfile={s} usejni=t ").format(
            maxh=self.get_mem(fraction=0.95),
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
