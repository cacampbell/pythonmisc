#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re


class GzipAll(ParallelCommand):
    def make_command(self, filename, choice):
        assert(choice.upper().strip() in ["ZIP", "UNZIP"])
        if choice.upper().strip() == "ZIP":
            output = re.sub(self.input_suffix,
                            "{}.gz".format(self.input_suffix), filename)
            return "gzip -c {i} > {o}".format(i=filename, o=output)
        else:
            output = re.sub(".gz", "", filename)
            return "gunzip -c {i} > {o}".format(i=filename, o=output)