#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class CleanSort(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(CleanSort, self).__init__(*args, **kwargs)

    def make_command(self, sam):
        # input unsorted, mapped sam file
        # output clean, sorted, mapped bam file
        output = self.rebase_file(sam)
        output = self.replace_extension_with(".clean.sam", output)
        output_f = self.rebase_file(sam)
        output_f = self.replace_extension_with(".sorted.sam", output_f)
        bam = self.rebase_file(sam)
        bam = self.replace_extension_with(".clean.sort.bam", bam)
        command = ("java -Xms{xms} -Xmx{xmx} -jar {picard} CleanSam INPUT={i} "
                   "OUTPUT={o} && java -Xms{xms} -Xmx{xmx} -jar {picard} "
                   "FixmateInformation I={o} O={o_f} SO=coordinate && "
                   "samtools -@ {stt} -m {stm} view -bS {o_f} > {bam}").format(
            xms=self.get_mem(fraction=0.90),
            xmx=self.get_mem(fraction=0.95),
            picard=self.picard,
            i=sam,
            o=output,
            o_f=output_f,
            stt=self.get_threads(),
            stm=self.get_mem(fraction=(1 / self.get_threads())),
            bam=bam
        )
        return (command)