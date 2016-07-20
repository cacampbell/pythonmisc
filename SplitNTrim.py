#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class SplitNTrim(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(SplitNTrim, self).__init__(*args, **kwargs)
        self.input_regex = ".*"
        self.read_regex = ".*"
        self.extension = ".bam"
        self.set_default("gatk", "GenomeAnalysisTK.jar")
        self.set_default("tmp_dir", "~/tmp")

    def make_command(self, bam):
        output = self.rebase_file(bam)
        output = self.replace_extension_with(".splitncigar.bam", output)
        command = ("java -Xmx{xmx} -Xms{xms} -Djava.io.tmpdir={tmpdir} "
                   "-jar {gatk} -T SplitNCigarReads -R reference.fasta "
                   "-I {inf} -O {outf} -U ALLOW_N_CIGARS -rf "
                   "ReassignOneMappingQuality -RMQF 255 -RMQT 60").format(
                       xmx=self.get_mem(fraction=0.95),
                       xms=self.get_mem(fraction=0.90),
                       tmpdir=self.tmp_dir,
                       gatk=self.gatk,
                       inf=bam,
                       outf=output
                   )
        return(command)
