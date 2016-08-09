#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class Assesser(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        self.input_regex = ".*"
        self.read_regex = ".*"
        self.extension = r".bam$"
        super(Assesser, self).__init__(*args, **kwargs)
        self.set_default("mode", None)
        self.set_default("reference", "reference.fa")
        self.set_default("ref_flat", "ref_flat")
        self.set_default("tmp_dir", "~/tmp")
        self.set_default("picard", "~/.prog/picard-tools-2.5.0/picard.jar")

    def make_command(self, read):
        # Call CollectAlignmentSummaryMetrics, CollectQualityYieldMetrics, and
        # CollectWGSMetrics if the input mode is not RNA
        output = self.rebase_file(read)
        alnsum = self.replace_extension_with(".aln_s.txt", output)
        qualyield = self.replace_extension_with(".qual_yld.txt", output)
        wgsmetrics = self.replace_extension_with(".wgs_met.txt", output)
        rnamet = self.replace_extension_with(".rna_met.txt")
        command = ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar "
                   "{picard} CollectAlignmentSummaryMetrics R={ref} I={input} "
                   "O={alnsum} && java -Xms{xms} -Xmx{xmx} "
                   "-Djava.io.tmpdir={tmp} -jar {picard} "
                   "CollectQualityYieldMetrics I={input} O={qualyield}").format(
            xms=self.get_mem(0.95),
            xmx=self.get_mem(0.98),
            tmp=self.tmp_dir,
            picard=self.picard,
            ref=self.reference,
            input=read,
            alnsum=alnsum,
            qualyield=qualyield
        )

        if self.mode.upper().strip() in ["DNA", "RNA"]:
            command += (" && java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} "
                        "-jar {picard} ").format(
                xms=self.get_mem(0.95),
                xmx=self.get_mem(0.98),
                tmp=self.tmp_dir,
                picard=self.picard
            )
            if self.mode.upper().strip() == "DNA":
                command += (" CollectWgsMetrics I={input} O={wgsmet} R={ref} "
                            "INCLUDE_BQ_HISTOGRAM").format(
                    input=read,
                    wgsmet=wgsmetrics,
                    ref=self.reference
                )
            elif self.mode.upper().strip() == "RNA":
                command += (" CollectRnaSeqMetrics I={input} O={rnamet} "
                            "REF_FLAT={refflat}").format(
                    input=read,
                    rnamet=rnamet,
                    refflat=self.ref_flat
                )

        return (command)
