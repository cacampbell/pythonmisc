#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand
from Bash import which


class ValidateMapped(PairedEndCommand):
    """
    Validate SAM or BAM files
    """

    def __init__(self, *args, **kwargs):
        super(ValidateMapped, self).__init__(*args, **kwargs)
        self.input_regex=".*"
        self.read_regex=".*"
        self.extension=".[s|b]am"
        self.set_default("bamutil", "bam")
        
    def make_command(self, filename):
        output = self.rebase_file(filename)
        output = self.replace_extension_with(".txt", output)
        command = ("{bam} validate --in {i} > {o}").format(bam=self.bamutil,
                                                           i=filename,
                                                           o=output)
        return (command)
