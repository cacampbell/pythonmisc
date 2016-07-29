#!/usr/bin/env python3
from os.path import splitext
from ParallelCommand import ParallelCommand


class Splitter(ParallelCommand):
    def __init__(self, *args, **kwargs):
        super(Splitter, self).__init__(*args, **kwargs)
        self.set_default("lines", "100000")

    def make_command(self, filename):
        self.prefix = splitext(filename)[0] + ".split."
        assert (int(self.lines) % 4 == 0)
        command = ("split -l {lines} {i} {o_pre}").format(
            lines=self.lines, i=filename, o_pre=self.prefix
        )  # Command
        return (command)
