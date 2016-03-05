#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class RecalibrateBases(ParallelCommand):
    def make_command(self, read):
        command = ""
        return command