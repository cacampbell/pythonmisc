#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class VelvetOasesAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(VelvetOasesAssemble, self).__init__(*args, **kwargs)

    def make_commands(self):
        pass
        # TODO: Run Velvet/Oases
