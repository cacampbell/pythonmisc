#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class SplitNTrim(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        self.modules = ['java']
        super(SplitNTrim, self).__init__(*args, **kwargs)
        self.input_regex = ".*"
        self.read_regex = ".*"
        self.extension = ".bam"
        self.set_default("gatk",
                         "/home/cacampbe/.prog/GenomeAnalysisTK/GenomeAnalysisTK.jar")
        self.set_default("tmp_dir", "~/tmp")
        self.set_default("reference", "reference.fa")

    def make_command(self, bam):
        output = self.rebase_file(bam)
        output = self.replace_extension_with(".trim.bam", output)
        # GATK recommends that STAR alignments have quality scores of 255
        # reassigned to a quality score of 60 (defualt), since STAR incorrectly
        # marks good alignments with a quality of 255. BBMap does not do this,
        # so this option is not called.
        # IF USING STAR, add the following to the command:
        # -rf ReassignOneMappingQuality -RM QF 255 -RMQT 60
        #
        # NOTE:
        # Adding -rf BadCigar to deal with some special cases in cigar reads
        command = ("java -Xmx{xmx} -Xms{xms} -Djava.io.tmpdir={tmpdir} "
                   "-jar {gatk} -T SplitNCigarReads -R {reference} -rf BadCigar"
                   " -I {inf} -o {outf} -U ALLOW_N_CIGAR_READS").format(
                       xmx=self.get_mem(fraction=0.95),
                       xms=self.get_mem(fraction=0.90),
                       tmpdir=self.tmp_dir,
                       gatk=self.gatk,
                       inf=bam,
                       outf=output,
                       reference=self.reference
                   )
        return(command)
