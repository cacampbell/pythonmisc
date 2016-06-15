#!/usr/bin/env python
from ParallelCommand import ParallelCommand


class CollectQCMetrics(ParallelCommand):
    def __init__(self, input_root, output_root):
        self.read_marker = "_R1"
        super(CollectQCMetrics, self).__init__(input_root, output_root)

    def make_command(self, read):
        pass
