#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class CleanSort(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(CleanSort, self).__init__(*args, **kwargs)

    def make_command(self, sam):
        # input unsorted, mapped sam file
        # output clean, sorted, mapped bam file
        output = self.rebase_file(sam)
        output = self.replace_extension_with(".tmp.sam")
        output_f = self.rebase_file(sam)
        command = ("java -Xms{xms} -Xmx{xmx} -jar {picard} CleanSam INPUT={i} "
                   "OUTPUT={o} && java -Xms{xms} -Xmx{xmx} -jar {picard} "
                   "FixmateInformation I={o} O={o_f} SO=coordinate").format(
            xms=self.get_mem(fraction=0.90),
            xmx=self.get_mem(fraction=0.95),
            picard=self.picard,
            i=sam,
            o=output,
            o_f=output_f
        )
        return (command)
