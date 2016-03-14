#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re


class GzipAll(ParallelCommand):
    def __init__(self, input_, output_, choice):
        self.choice = choice
        super(GzipAll, self).__init__()

    def make_command(self, filename):
        assert(self.choice.upper().strip() in ["ZIP", "UNZIP"])
        if self.choice.upper().strip() == "ZIP":
            output = re.sub(self.input_suffix,
                            "{}.gz".format(self.input_suffix), filename)
            return "gzip -c {i} > {o}".format(i=filename, o=output)
        else:
            output = re.sub(".gz", "", filename)
            return "gunzip -c {i} > {o}".format(i=filename, o=output)