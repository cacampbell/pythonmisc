#!/usr/bin/env bash
from ParallelCommand import ParallelCommand
import re

class MergeBamAlignment(ParallelCommand):
    def __init__(self, input_, output_):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(MergeBamAlignment, self).__init__(input_, output_)

    def make_command(self, read):
        pass


