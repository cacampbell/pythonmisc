#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand
import re


class Splitter(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Splitter, self).__init__(*args, **kwargs)
        self.set_default("lines", "8000000")
        self.read_regex = ".*"
        self.set_default("extension", ".fq$")
        self.changed_ext = False

    def make_command(self, filename):
        output = self.rebase_file(filename)
        header = self.replace_extension_with(".header.sam", filename)
        self.prefix = self.replace_extension_with(".split.", output)
        assert (int(self.lines) % 8 == 0)  # Assume paired end to be safe
        extension = self.extension

        patterns = ["\$", "\^", "\*"]
        for pattern in patterns:
            try:
                extension = re.sub(pattern, "", extension)
            except:
                pass

        try:
            extension = re.sub("\.", "\\.", extension)
        except:
            pass

        if filename.endswith(".fq") or filename.endswith(".fastq"):
            command = ("split -l {lines} {i} {o_pre} &&"
                       " rename \"s/\$/{ext}/\" {o_pre}*").format(
                lines=self.lines,
                i=filename,
                o_pre=self.prefix,
                ext=extension
            )  # Command
            return(command)

        elif filename.endswith(".bam"):
            command = ("samtools view -H {i} > {h} && samtools view -b {i} | "
                       "split -l {lines} - {o_pre} && "
                       "samtools reheader {h} {o_pre}* && rename "
                       "\"s/$/{ext}/\" {o_pre}*").format(
                           i=filename,
                           h=header,
                           lines=self.lines,
                           o_pre=self.prefix,
                           ext=extension
                       )
            return (command)
        elif filename.endswith(".sam"):
            command = ("samtools view -H {i} > {h} && samtools view {i} | "
                       "split -l {lines} - {o_pre} && find -L . "
                       "| grep {o_pre} | parallel --gnu -j4 \"cat {h} "
                       "{{}} > {{}}.tmp && rm -f {{}}\" && rename "
                       "\"s/\.tmp$/{ext}/\" {o_pre}*").format(
                           i=filename,
                           h=header,
                           lines=self.lines,
                           o_pre=self.prefix,
                           ext=extension
                       )
            return(command)
