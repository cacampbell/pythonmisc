#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand


class Deduplicate(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Deduplicate, self).__init__(*args, **kwargs)
        self.set_default("picard", "~/.prog/picard-tools-2.4.1/picard.jar")
        self.set_default("use_picard", False)
        self.set_default("by_mapping", False)

    def make_command(self, bam):
        if self.by_mapping:
            if self.use_picard:
                output = self.replace_extension_with(".dedupe.bam", bam)
                output = self.rebase_file(output)

                command = (
                "java -Xms{xms} -Xmx{xmx} -jar {picard} MarkDuplicates "
                "INPUT={i} OUTPUT={o} REMOVE_DUPLICATES=true "
                "MAX_RECORDS_IN_RAM=50000 ASSUME_SORTED=true").format(
                    xms=self.get_mem(0.90),
                    xmx=self.get_mem(0.95),
                    picard=self.picard,
                    i=bam,
                    o=output)
                return (command)
            else:  # Not using Picard, using BBMap
                output = self.replace_extension_with(".dedupe.bam", bam)
                output = self.rebase_file(output)
                command = ("dedupebymapping.sh -Xmx{xmx} threads={t} "
                           "in={i} out={o} monitor=600,0.01").format(
                    xmx=self.get_mem(fraction=0.95),
                    t=self.get_threads(),
                    i=bam,
                    o=output
                )
                return (command)
        else:  # Ignoring picard, deduplicating reads without mapping (BBMap)
            read = bam  # For clarity, does nothing
            mate = self.mate(read)
            out1 = self.rebase_file(read)
            out2 = self.rebase_file(mate)
            command = ("dedupe.sh in={i1} out={o1} "
                       "-Xmx{xmx} threads={t} monitor=600,0.01 && "
                       "dedupe.sh in={i2} out={o2} "
                       "-Xmx{xmx} threads={t} monitor=600,0.01").format(
                i1=read,
                i2=mate,
                o1=out1,
                o2=out2,
                xmx=self.get_mem(fraction=0.95),
                t=self.get_threads()
            )
            return (command)
