#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class SOAPdenovoAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(SOAPdenovoAssemble, self).__init__(*args, **kwargs)

    def make_commands(self):
        pass
        # TODO: Run SOAPdenovo
