#!/usr/bin/env python
from ParallelCommand import ParallelCommand
import re


class CollectQCMetrics(ParallelCommand):
    def __init__(self, input_, output_):
        self.read_marker = "_R1"
        super(CollectQCMetrics, self).__init__(input_, output_)

    def make_command(self, read):
        pass
