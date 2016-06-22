#!/usr/bin/env python
from PairedEndCommand import PairedEndCommand
from sys import stderr


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
                           "in={i} out={o} monitor=600,0.01 usejni=t").format(
                    xmx=self.get_mem(fraction=0.95),
                    t=self.get_threads(),
                    i=bam,
                    o=output
                )
                return (command)
        else:  # Ignoring picard, deduplicating reads without mapping (FastUniq)
            read = bam  # For clarity, does nothing
            mate = self.mate(read)
            pipe1 = self.replace_extension_with(".pipe", read)
            pipe2 = self.replace_extension_with(".pipe", mate)
            file_list = self.replace_read_marker_with("_filelist", read)
            out1 = self.rebase_file(read)
            out2 = self.rebase_file(mate)
            command = ("mkfifo {p1} && mkfifo {p2} && gunzip -c {i1} > {p1} && "
                       "gunzip -c {i2} > {p2} && fastuniq -i {fl} -o {o1} -p "
                       "{o2} -t q").format(i1=read, i2=mate, p1=pipe1, p2=pipe2,
                                           fl=file_list, o1=out1, o2=out2)

            if not self.dry_run:
                with open(file_list, "w+") as fh:
                    print(pipe1, file=fh)
                    print(pipe2, file=fh)

                if self.verbose:
                    print("Wrote {} for FastUniq".format(file_list),
                          file=stderr)
            else:
                if self.verbose:
                    print("Would have made {} if not dry_run".format(file_list),
                        file=stderr)

            return (command)
