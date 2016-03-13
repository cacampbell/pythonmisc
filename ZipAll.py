#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re
import os


class ZipAll(ParallelCommand):
    def __init__(self, input_, output_, choice):
        self.choice = choice
        super(ZipAll, self).__init__()

    def make_command(self, filename):
        assert(self.choice.upper().strip() in ["ZIP", "UNZIP"])
        if self.choice.upper().strip() == "ZIP":
            output = re.sub(self.input_suffix,
                            "{}.zip".format(self.input_suffix), filename)
            return "zip -r {o} {i}".format(i=filename, o=output)
        else:
            output = re.sub(".zip", "", filename)
            output = os.path.dirname(output)
            return "unzip {i} -d {o}".format(i=filename, o=output)
