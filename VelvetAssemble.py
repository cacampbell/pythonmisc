#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class VelvetAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(VelvetAssemble, self).__init__(*args, **kwargs)

    def make_commands(self):
        pass
        # TODO: Run Velvet/Oases
