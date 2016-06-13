#!/usr/bin/env bash
from ParallelCommand import ParallelCommand


class MergeBamAlignment(ParallelCommand):
    def __init__(self, input_root, output_root):
        super(MergeBamAlignment, self).__init__(input_root, output_root)

    def make_command(self, read):
        pass