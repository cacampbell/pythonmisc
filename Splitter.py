#!/usr/bin/env python3
from os.path import splitext

from ParallelCommand import ParallelCommand


class Splitter(ParallelCommand):
    def __init__(self, *args, **kwargs):
        super(Splitter, self).__init__(*args, **kwargs)
        self.set_default("lines", "10000000")

    def make_command(self, filename):
        output = self.rebase_file(filename)
        self.prefix = splitext(output)[0] + ".split."
        assert (int(self.lines) % 4 == 0)
        command = ("split -l {lines} {i} {o_pre} &&"
                   " rename \"s/(.*)/$1{ext}/\" {o_pre}*").format(
            lines=self.lines,
            i=filename,
            o_pre=self.prefix,
            ext=self.extension
        )  # Command
        return (command)
