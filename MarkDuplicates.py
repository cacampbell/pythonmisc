#!/usr/bin/env python
import re

from ParallelCommand import ParallelCommand


class MarkDuplicates(ParallelCommand):
    def __init__(self, input_root, output_root):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(MarkDuplicates, self).__init__(input_root, output_root)

    def make_command(self, read):
        metrics = re.sub(self.read_marker, "_metrics", read)
        deduped = re.sub(self.input_suffix,
                         ".dedupe.{}".format(self.input_suffix), read)
        picard_jar = "~/picardtools/dist/picard.jar"
        command = ("java -Xms{xms} -Xmx{xmx} -jar {picard} MarkDuplicates"
                   "INPUT={i} OUTPUT={o} REMOVE_DUPLICATES=true "
                   "MAX_RECORDS_IN_RAM=50000 ASSUME_SORTED=true "
                   "METRICS_FILE={metrics}").format(xms=self.get_mem(0.90),
                                                    xmx=self.get_mem(0.95),
                                                    picard=picard_jar,
                                                    i=read,
                                                    o=deduped,
                                                    metrics=metrics)
        return command
