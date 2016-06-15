#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class MarkDuplicates(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(MarkDuplicates, self).__init__(*args, **kwargs)
        self.picard = "~/.prog/picard-tools-2.4.1/picard.jar"

    def make_command(self, bam):
        output = self.replace_extension_with(".dedupe.bam", bam)
        output = self.rebase_file(output)
        command = ("java -Xms{xms} -Xmx{xmx} -jar {picard} MarkDuplicates "
                   "INPUT={i} OUTPUT={o} REMOVE_DUPLICATES=true "
                   "MAX_RECORDS_IN_RAM=50000 ASSUME_SORTED=true").format(
            xms=self.get_mem(0.90),
            xmx=self.get_mem(0.95),
            picard=self.picard,
            i=bam,
            o=output)
        return (command)
