#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class BQSR(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(BQSR, self).__init__(*args, **kwargs)
        self.set_default("known", "known_variants.vcf")
        self.set_default("recal_table", None)
        self.set_default("reference", "reference.fa")
        self.set_default("tmp_dir", "~/tmp")
        self.set_default("GATK",
                         "~/.prog/GenomeAnalysisTK/GenomeAnalysisTK.jar")

    def make_command(self, bam_file):
        """
        Run GATK Base Quality Score Recalibration protocol, using a combination
        of BaseRecalibrator, PrintReads, and AnalyzeCovariates to adjust
        systematic errors in Base Quality Scores.

        Reason: Quality Scores output by sequencing machines are largely
        inaccurate, which leads to bad base calls. Correcting these errors leads
        to less aberrant calls, and to more accurate calls.
        :param bam_file: The input BAM file (deduplicated and merged by sample)
        :return: str: command string to be wrapped
        """
        command = ""
        var_sites = "-knownSites {}".format(self.known)
        obam = self.rebase_file(bam_file)

        if not self.recal_table:
            self.recal_table = self.rebase_file(bam_file)
            self.recal_table = self.replace_extension_with(".grp", bam_file)
            command += ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar"
                        " {GATK} -T BaseRecalibrator -R {ref} -I {ibam} -o "
                        "{recal_table} {knownsites} && ").format(
                xms=self.get_mem(0.98),
                xmx=self.get_mem(0.99),
                tmp=self.tmp_dir,
                GATK=self.GATK,
                ref=self.reference,
                ibam=bam_file,
                recal_table=self.recal_table,
                knownsites=var_sites
            )  # Create Recalibration Table

        command += ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar "
                    "{GATK} -T PrintReads -R {ref} -I {ibam} -BQSR "
                    "{recal_table} -o {obam}").format(
            xms=self.get_mem(0.98),
            xmx=self.get_mem(0.99),
            tmp=self.tmp_dir,
            GATK=self.GATK,
            ref=self.reference,
            ibam=bam_file,
            recal_table=self.recal_table,
            obam=obam
        )  # Recalibrate the BAM file

        return (command)

# TODO: Add commands for making plots of recalibration using double recal strat
