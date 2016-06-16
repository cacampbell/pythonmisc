#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class CleanSort(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(CleanSort, self).__init__(*args, **kwargs)

    def make_command(self, bam):
        # input unsorted, mapped bam file
        # output clean, sorted, mapped bam file
        output = self.rebase_file(bam)
        command = ("java -Xms{xms} -Xmx{xmx} -jar {picard} FixmateInformation "
                   "I={i} O={o} SO=coordinate").format(
            xms=self.get_mem(fraction=0.90),
            xmx=self.get_mem(fraction=0.95),
            picard=self.picard,
            i=bam,
            o=output
        )
        return (command)
