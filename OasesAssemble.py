#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


# TODO

class OasesAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(OasesAssemble, self).__init__(*args, **kwargs)

    def make_commands(self):
        pass
        # TODO: Run Velvet/Oases
