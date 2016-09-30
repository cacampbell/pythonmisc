#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


# TODO: Create Somatic Caller, if need be


class HaplotypeCaller(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(HaplotypeCaller, self).__init__(*args, **kwargs)
        self.set_default("reference", "reference.fa")
        self.set_default("tmp_dir", "~/tmp")
        self.set_default("GATK",
                         "~/.prog/GenomeAnalysisTK/GenomeAnalysisTK.jar")
        self.set_default("mode", "DNA")
        self.set_default("input_regex", ".*")
        self.set_default("read_regex", ".*")
        self.set_default("extension", ".bam$")
        self.set_default("dbsnp", None)
        self.set_default("intervals", None)

    def make_command(self, filename):
        """
        Create gVCF HaplotypeCaller command.
        :param filename: bam file
        :return: string
        """
        gvcf = self.rebase_file(filename)
        gvcf = self.replace_extension_with(".g.vcf", gvcf)
        threads = self.get_threads()
        nct = threads if int(threads) < 9 else "8"
        command = ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar "
                   "{gatk} -T HaplotypeCaller -I {bam} -o {gvcf}"
                   " -R {ref} --emitRefConfidance GVCF -nct {nct}").format(
            xms=self.get_mem(0.98),
            xmx=self.get_mem(0.99),
            tmp=self.tmp_dir,
            gatk=self.GATK,
            bam=filename,
            gvcf=gvcf,
            ref=self.reference,
            nct=nct
        )  # HaplotypeCaller command

        if self.mode.upper().strip() == "RNA":
            command += (" -dontUseSoftClippedBases -stand_call_conf 20.0 "
                        "-stand_emit_conf 20.0")

        if self.dbsnp:
            command += " --dbsnp {}".format(self.dbsnp)

        if self.intervals:
            command += " -L {}".format(self.intervals)

        return (command)
