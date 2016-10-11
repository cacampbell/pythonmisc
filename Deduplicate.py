#!/usr/bin/env python
from sys import stderr

from PairedEndCommand import PairedEndCommand


class Deduplicate(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Deduplicate, self).__init__(*args, **kwargs)
        self.set_default("picard", "~/.prog/picard-tools-2.5.0/picard.jar")
        self.set_default("use_picard", False)
        self.set_default("use_samtools", False)
        self.set_default("by_mapping", False)
        self.set_default("fastuniq", False)
        self.set_default("tmp_dir", "~/tmp")

    def __picard_dedupe(self, bam):
        output = self.replace_extension_with(".dedupe.bam", bam)
        output = self.rebase_file(output)
        metrics = self.replace_extension_with(".txt", output)
        command = (
            "java -Djava.io.tmpdir={tmp} -Xms{xms} -Xmx{xmx} -jar {picard} "
            "MarkDuplicates INPUT={i} OUTPUT={o} REMOVE_DUPLICATES=true "
            "MAX_RECORDS_IN_RAM=50000 "
            "ASSUME_SORTED=true METRICS_FILE={stats}").format(
            xms=self.get_mem(0.94),
            xmx=self.get_mem(0.95),
            picard=self.picard,
            i=bam,
            o=output,
            tmp=self.tmp_dir,
            stats=metrics
        )  # Command
        return (command)

    def __dedupe_by_mapping(self, bam):
        output = self.replace_extension_with(".dedupe.bam", bam)
        output = self.rebase_file(output)
        command = ("dedupebymapping.sh -Xmx{xmx} threads={t} "
                   "in={i} out={o} usejni=t").format(
            xmx=self.get_mem(fraction=0.99),
            t=self.get_threads(),
            i=bam,
            o=output
        )
        return (command)

    def __samtools_dedupe(self, bam):
        output = self.replace_extension_with(".dedupe.bam", bam)
        output = self.rebase_file(output)
        command = ("samtools rmdup {i} {o}").format(i=bam, o=output)
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
        command = ("dedupe.sh in={i} in2={m} out=STDOUT.fq -Xmx{xmx} "
                   "threads={t} usejni=t | reformat.sh -Xmx{xmx} in=STDIN.fq "
                   "int out1={o1} out2={o2}").format(i=read,
                                                     m=mate,
                                                     o1=out,
                                                     o2=out2,
                                                     xmx=self.get_mem(0.98),
                                                     t=self.get_threads())
        return (command)

    def make_command(self, bam):
        if self.by_mapping:
            if self.use_picard:
                return (self.__picard_dedupe(bam))
            elif self.use_samtools:
                return (self.__samtools_dedupe(bam))
            else:  # Not using Picard, using BBMap
                return (self.__dedupe_by_mapping(bam))
        else:  # Ignoring picard, deduplicating reads without mapping (FastUniq)
            if self.fastuniq:
                return (self.__dedupe_fastuniq(bam))
            else:
                return (self.__dedupe(bam))