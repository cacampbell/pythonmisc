#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class CleanSort(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(CleanSort, self).__init__(*args, **kwargs)
        self.input_regex = ".*"
        self.read_regex = ".*"
        self.extension = ".sam"
        self.set_default("picard", "~/.prog/picard-tools-2.5.0/picard.jar")
        self.set_default("tmp_dir", "~/tmp")

    def make_command(self, sam):
        # input unsorted, mapped sam file
        # output clean, sorted, mapped bam file
        output = self.rebase_file(sam)
        output = self.replace_extension_with(".clean.sam", output)
        output_f = self.rebase_file(sam)
        output_f = self.replace_extension_with(".clean.sort.sam", output_f)
        bam = self.rebase_file(sam)
        bam = self.replace_extension_with(".clean.sort.bam", bam)
        command = ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmpdir} "
                   "-XX:ParallelGCThreads={gc} -jar {picard} "
                   "CleanSam INPUT={i} TMP_DIR={tmpdir} OUTPUT={o} && java "
                   "-Djava.io.tmpdir={tmpdir} -Xms{xms} -Xmx{xmx} "
                   "-jar {picard} FixMateInformation I={o} O={o_f} "
                   "TMP_DIR={tmpdir} SO=coordinate && samtools view -@ {stt} "
                   "-m {stm} -bS {o_f} > {bam}").format(
            xms=self.get_mem(fraction=0.90),
            xmx=self.get_mem(fraction=0.95),
            gc=self.get_threads() if int(self.get_threads()) <= 8 else "8",
            tmpdir=self.tmp_dir,
            picard=self.picard,
            i=sam,
            o=output,
            o_f=output_f,
            stt=self.get_threads() if int(self.get_threads()) <= 8 else "8",
            stm=self.get_mem(fraction=(1 / int(self.get_threads()))),
            bam=bam
        )
        return (command)
