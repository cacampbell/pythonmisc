#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re


class RsyncAllLocal(ParallelCommand):
    def make_command(self, filename):
        """
        Make command to move a single local file to a single output file,
        using Rsync.
        """
        output = self.output_file(filename)

