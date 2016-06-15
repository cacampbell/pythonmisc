#!/usr/bin/env bash
from PairedEndCommand import PairedEndCommand


class MergeBamAlignment(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(MergeBamAlignment, self).__init__(*args, **kwargs)

    def make_command(self, read):
        pass