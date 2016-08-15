#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class IndelRealigner(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(IndelRealigner, self).__init__(*args, **kwargs)
        self.modules = ["java"]
        self.set_default("tmp", "~/tmp")
        self.set_default("gatk",
                         "~/.prog/GenomeAnalysisTK/GenomeAnalysisTK.jar")
        self.set_default("reference", "reference.fa")
        self.set_default("known", None)

    def make_command(self, filename):
        def __known_sites():
            if self.known:
                return "-known {}".format(self.known)
            return ""

        realigner_targets = self.rebase_file(filename)
        realigner_targets = self.replace_extension_with(".intervals",
                                                        realigner_targets)
        output_bam = self.rebase_file(filename)
        command = ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar "
                   "{gatk} -T RealignerTargetCreator -R {ref} -I {inbam} "
                   "-o {targets} {known} && java -Xms{xms} -Xmx{xmx} "
                   "-Djava.io.tmpdir={tmp} -jar {gatk} -T IndelRealigner -R "
                   "{ref} -I {inbam} -targetIntervals {targets} -o {outputb}"
                   " {known}").format(
            xms=self.get_mem(0.95),
            xmx=self.get_mem(0.99),
            tmp=self.tmp,
            gatk=self.gatk,
            ref=self.reference,
            inbam=filename,
            targets=realigner_targets,
            outputb=output_bam,
            known=__known_sites()
        )

        return command
