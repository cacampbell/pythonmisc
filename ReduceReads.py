#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class ReduceReads(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(ReduceReads, self).__init__(*args, **kwargs)
        self.set_default("reference", "reference.fa")
        self.set_default("tmp_dir", "~/tmp")
        self.set_default("GATK",
                         "~/.prog/GenomeAnalysisTK/GenomeAnalysisTK.jar")

    def make_command(self, filename):
        """
        Use GATK ReduceReads to compress BAM file into variant regions only
        :param filename: input BAM file
        :return: command string
        """
        outfile = self.rebase_file(filename)
        command = ("java -xms{xms} -xmx{xmx} -Djava.io.tmpdir={tmp} -jar "
                   "{gatk} -T ReduceReads -R {ref} -I {i} -O {o}").format(
            xms=self.get_mem(0.98),
            xmx=self.get_mem(0.99),
            tmp=self.tmp_dir,
            gatk=self.GATK,
            ref=self.reference,
            i=filename,
            o=outfile
        )  # ReduceReads command

        return (command)
