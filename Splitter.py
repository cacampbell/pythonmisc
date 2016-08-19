#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class Splitter(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Splitter, self).__init__(*args, **kwargs)
        self.set_default("lines", "8000000")
        self.read_regex = ".*"

    def make_command(self, filename):
        output = self.rebase_file(filename)
        header = self.replace_extension_with(".header.sam", filename)
        self.prefix = self.replace_extension_with(".split.", output)
        assert (int(self.lines) % 8 == 0)  # Assume paired end to be safe

        if filename.endswith(".fq") or filename.endswith(".fastq"):
            command = ("split -l {lines} {i} {o_pre} &&"
                       " rename \"s/(.*)/$1{ext}/\" {o_pre}*").format(
                lines=self.lines,
                i=filename,
                o_pre=self.prefix,
                ext=self.extension
            )  # Command
            return(command)

        elif filename.endswith(".bam"):
            command = ("samtools view -H {i} > {h} && samtools view -b {i} | "
                       "split -l {lines} - {o_pre} && "
                       "samtools reheader {h} {o_pre}* && rename "
                       "\"s/$/\{ext}/\" {o_pre}*").format(
                           i=filename,
                           h=header,
                           lines=self.lines,
                           o_pre=self.prefix,
                           ext=self.extension
                       )
            return (command)
        elif filename.endswith(".sam"):
            command = ("samtools view -H {i} > {h} && samtools view {i} | "
                       "split -l {lines} - {o_pre} && find -L -max_depth 1 . "
                       "-iname \"{o_pre}*\" | parallel --gnu -j4 \"echo {h} "
                       "{{}} > {{}}.tmp && mv {{}}.tmp {{}}\" && rename "
                       "\"s/$/\{ext}/\" {o_pre}*").format(
                           i=filename,
                           h=header,
                           lines=self.lines,
                           o_pre=self.prefix,
                           ext=self.extension
                       )
            return(command)
