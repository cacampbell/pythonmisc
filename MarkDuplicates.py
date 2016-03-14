#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class MarkDuplicates(ParallelCommand):
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