#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class TadpoleAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(TadpoleAssemble, self).__init__(*args, **kwargs)

    def make_command(self, files):
        pass
