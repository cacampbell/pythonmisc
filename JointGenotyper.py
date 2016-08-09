#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class JointGenotyper(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(self, JointGenotyper).__init__(*args, **kwargs)

    def make_command(self, read):
        pass

    def format_commands(self):
        pass

    def run(self):
        pass
