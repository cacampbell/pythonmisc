#!/usr/bin/env python3
from os.path import splitext

from ParallelCommand import ParallelCommand


class Splitter(ParallelCommand):
    def __init__(self, *args, **kwargs):
        super(Splitter, self).__init__(*args, **kwargs)
        self.set_default("lines", "100000")

    def make_command(self, filename):
        self.prefix = splitext(filename)[0] + ".split."
        assert (self.lines % 4 == 0)
        command = ("split -l {} {i} {o_pre}").format(self.lines,
                                                     filename,
                                                     self.prefix)
        return (command)
