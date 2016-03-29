#!/usr/bin/env python
from __future__ import print_function
from ParallelCommand import ParallelCommand
import re


class BBMapper(ParallelCommand):
    def __init__(self, input_, ouptut_):
        self.read_marker = "_1"
        self.mate_marker = "_2"
        super(BBMapper, self).__init__(input_, output_)

    def make_command(self, read):
        mate = re.sub(self.read_marker, self.mate_marker, read)
        out1 = output_file(read)
        out2 = output_file(mate)
        decompress_out1 = re.sub(".gz", "", out1)
        decompress_out2 = re.sub(".gz", "", out2)
        command = ("reformat.sh in1={} in2={} out1={}
                     out2={} reads=4").format(read, mate, 
                                              decompress_out1, 
                                              decompress_out2)
        return command
