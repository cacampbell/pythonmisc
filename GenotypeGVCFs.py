#!/usr/bin/env python3
from sys import stderr

from os.path import join

from PairedEndCommand import PairedEndCommand


class GenotypeGVCFs(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(GenotypeGVCFs, self).__init__(*args, **kwargs)
        self.set_default("reference", "reference.fa")
        self.set_default("tmp_dir", "~/tmp")
        self.set_default("GATK",
                         "~/.prog/GenomeAnalysisTK/GenomeAnalysisTK.jar")
        self.set_default("input_regex", ".*")
        self.set_default("read_regex", ".*")
        self.set_default("extension", ".g.vcf$")

    def format_commands(self):
        job_name = "Genotype_GVCFs_{}".format(self.cluster_options['job_name'])
        command = ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar "
                   "{gatk} -T GenotypeGVCFs -R {ref} -o {vcf}").format(
            xms=self.get_mem(0.98),
            xmx=self.get_mem(0.99),
            tmp=self.tmp_dir,
            gatk=self.GATK,
            ref=self.reference,
            vcf=join(self.output_root, "snps.indels.svs.raw.vcf")
        )
        for filename in self.files:
            command += (" --variant {}".format(filename))

        if self.verbose:
            print(command, file=stderr)

        self.commands[job_name] = command

    def make_command(self, filename):
        pass
