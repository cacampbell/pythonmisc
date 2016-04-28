#!/usr/bin/env bash
from ParallelCommand import ParallelCommand


class MergeBamAlignment(ParallelCommand):
    def __init__(self, input_root, output_root):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(MergeBamAlignment, self).__init__(input_root, output_root)

    def make_command(self, read):
        pass


