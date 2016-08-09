#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class VariantCaller(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(self, VariantCaller).__init__(*args, **kwargs)
        self.set_default("mode", "DNA")
        self.set_default("dbsnp", None)
        self.set_default("intervals", None)
        self.set_deafult("GATK",
                         "~/.prog/GenomeAnalysisTK/GenomeAnalysisTK.jar")
        self.set_default("reference", "reference.fa")
        self.set_default("tmp_dir", "~/tmp")

    def make_command(self, read):
        output = self.replace_extension_with(".raw.snps.indels.g.vcf", read)
        output = self.rebase_file(output)
        command = ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar {gatk}"
                   " -T HaploTypeCaller -R {ref} -I {i} --emitConfidence GVCF"
                   " -o {o} -nct {t}").format(
                       xms=self.get_mem(0.95),
                       xmx=self.get_mem(0.99),
                       tmp=self.tmp_dir,
                       gatk=self.GATK,
                       ref=self.reference,
                       i=read,
                       o=output,
                       t=self.threads()
                   )  # Variant Calling Base

        if self.dbsnp:
            command += " --dbsnp {}".format(self.dbsnp)

        if self.intervals:
            command += " -L {}".format(self.intervals)

        return(command)
