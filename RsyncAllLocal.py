#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re


class RsyncAllLocal(ParallelCommand):
    def make_command(self, filename):
        output = self.output_file(filename)
        command = ("rsync -avzh {} {}".format(filename, output))
        return command
