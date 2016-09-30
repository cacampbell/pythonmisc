#!/usr/bin/env python3
import re

from PairedEndCommand import PairedEndCommand


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
            # MONSTROUS COMMAND TO SPLIT FQ FILES
            # use combination of paste and subprocesses to interleave
            # (10x faster than python)
            # Then, split the files into a multiple of 8 lines, and rename them
            mate = self.replace_read_marker_with("_R2", filename)
            interleaved = self.replace_read_marker_with("_pe", filename)
            o_interleaved = self.rebase_file(interleaved)
            self.prefix = self.replace_extension_with(".split.", o_interleaved)
            interleave_command = ("paste <(paste - - - - < {read}) "
                                  "<(paste - - - - < {mate}) "
                                  "| tr '\t' '\n' > {interleave}").format(
                read=filename, mate=mate, interleave=interleaved)
            split_command = ("split -l {l} {interleave} {o_pre}").format(
                l=self.lines, interleave=interleaved, o_pre=self.prefix)
            deinterleave_command = ("ls | grep {o_pre} |"
                                    " parallel --gnu -j{cpus} "
                                    "\"paste - - - - - - - - < {{}} |"
                                    " tee >(cut -f 1-4 |"
                                    " tr '\t' '\n' > {{}}.R1) |"
                                    " cut -f 5-8 |"
                                    " tr '\t' '\n' > {{}}.R2\"").format(
                o_pre=self.prefix, cpus=self.get_threads())
            rename_command = ("rename \"s/$/.fq/\" {}").format(self.prefix)
            command = ("{} && {} && {} && {}").format(interleave_command,
                                                      split_command,
                                                      deinterleave_command,
                                                      rename_command)
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
