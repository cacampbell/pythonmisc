#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class QualityControl(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(QualityControl, self).__init__(*args, **kwargs)
        self.set_default("reference",
                         "/home/cacampbe/.prog/bbmap/resources/adapters.fa")

    def make_command(self, read):
        mate = self.mate(read)
        output1 = self.rebase_file(read)
        output1 = self.replace_extension_with(".tmp.fq.gz", output1)
        output2 = self.rebase_file(mate)
        output2 = self.replace_extension_with(".tmp.fq.gz", output2)
        output3 = self.rebase_file(read)
        output4 = self.rebase_file(mate)
        stats1 = self.replace_read_marker_with("_stats1", read)
        stats1 = self.replace_extension_with(".txt", stats1)
        stats1 = self.rebase_file(stats1)
        stats2 = self.replace_read_marker_with("_stats2", read)
        stats2 = self.replace_extension_with(".txt", stats2)
        stats2 = self.rebase_file(stats2)
        # First Run
        # force trim module 5 -- gets rid of extra bases
        # trim matching adapters with k = 27 and min k = 11 on ends
        # allow up to two mismatches
        # force trim pairs to same length
        # trim by pair overlap detection
        # trim adapters from the right (3' adapters)
        #
        # Second Run
        # kmer-trim size 23 or above matches to the reference, with end
        # trimming of matches of 11 base pairs or more, with up to zero
        # mismatches with the reference.
        # Force trim both sets of a pair to the same length (tpe)
        # Trim adapters by pair overlap detection (tbo)
        # Trim adapters from the right (3' adapters)
        # trim quality from both ends, using quality score 5 to filter
        command = ("bbduk.sh -Xmx{xmx} threads={t} usejni=t monitor=600,0.01 "
                   "in1={i1} in2={i2} out1={o1} out2={o2} ftm=5 "
                   "stats={s1} ref={a} ktrim=r k=27 mink=11 hdist=2 tpe tbo; "
                   "bbduk.sh -Xmx{xmx} threads={t} usejni=t "
                   "in1={o1} in2={o2} out1={o3} out2={o4} stats={s2} "
                   "ref={a} ktrim=r k=23 mink=11 hdist=0 tpe tbo "
                   "qtrim=rl trimq=5 monitor=600,0.01").format(
            xmx=self.get_mem(fraction=0.95),
            t=self.get_threads(),
            i1=read,
            i2=mate,
            o1=output1,
            o2=output2,
            o3=output3,
            o4=output4,
            s1=stats1,
            s2=stats2,
            a=self.reference)
        return command
