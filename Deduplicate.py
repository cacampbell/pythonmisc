#!/usr/bin/env python
from sys import stderr

from PairedEndCommand import PairedEndCommand


class Deduplicate(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Deduplicate, self).__init__(*args, **kwargs)
        self.set_default("picard", "~/.prog/picard-tools-2.4.1/picard.jar")
        self.set_default("use_picard", False)
        self.set_default("by_mapping", False)
        self.set_default("fastuniq", False)

    def __picard_dedup(self, bam):
        output = self.replace_extension_with(".dedupe.bam", bam)
        output = self.rebase_file(output)

        command = (
            "java -Xms{xms} -Xmx{xmx} -jar {picard} MarkDuplicates "
            "INPUT={i} OUTPUT={o} REMOVE_DUPLICATES=true CREATE_INDEX=true "
            "MAX_RECORDS_IN_RAM=50000 ASSUME_SORTED=true").format(
            xms=self.get_mem(0.90),
            xmx=self.get_mem(0.95),
            picard=self.picard,
            i=bam,
            o=output)
        return (command)

    def __dedupe_by_mapping(self, bam):
        output = self.replace_extension_with(".dedupe.bam", bam)
        output = self.rebase_file(output)
        command = ("dedupebymapping.sh -Xmx{xmx} threads={t} "
                   "in={i} out={o} usejni=t").format(
            xmx=self.get_mem(fraction=0.95),
            t=self.get_threads(),
            i=bam,
            o=output
        )
        return (command)

    def __dedupe_fastuniq(self, read):
        mate = self.mate(read)
        file_list = self.replace_read_marker_with("_filelist", read)
        file_list = self.replace_extension_with(".txt", file_list)
        out1 = self.rebase_file(read)
        out2 = self.rebase_file(mate)
        command = ("fastuniq -i {fl} -o {o1} -p "
                   "{o2} -t q").format(fl=file_list, o1=out1, o2=out2)

        if not self.dry_run:
            with open(file_list, "w+") as fh:
                print(read, file=fh)
                print(mate, file=fh)

            if self.verbose:
                print("Wrote {} for FastUniq".format(file_list),
                      file=stderr)
        else:
            if self.verbose:
                print("Would have made {} if not dry_run".format(file_list),
                      file=stderr)

        return (command)

    def __dedupe(self, read):
        out = self.rebase_file(read)
        mate = self.mate(read)
        out2 = self.rebase_file(mate)
        command1 = ("dedupe.sh in={i} out={o} -Xmx{xmx} threads={t} "
                    "usejni=t").format(i=read,
                                       o=out,
                                       xmx=self.get_mem(fraction=0.95),
                                       t=self.get_threads())
        command2 = ("dedupe.sh in={i} out={o} -Xmx{xmx} threads={t} "
                    "usejni=t").format(i=mate,
                                       o=out2,
                                       xmx=self.get_mem(fraction=0.95),
                                       t=self.get_threads())
        return ([command1, command2])

    def make_command(self, bam):
        if self.by_mapping:
            if self.use_picard:
                return (self.__picard_dedeup(bam))
            else:  # Not using Picard, using BBMap
                return (self.__dedupe_by_mapping(bam))
        else:  # Ignoring picard, deduplicating reads without mapping (FastUniq)
            if self.fastuniq:
                return (self.__dedupe_fastuniq(bam))
            else:
                return (self.__dedupe(bam))
