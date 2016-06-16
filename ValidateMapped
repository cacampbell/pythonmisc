#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class ValidateMapped(PairedEndCommand):
    """
    Validate SAM or BAM files
    """

    def __init__(self, *args, **kwargs):
        super(ValidateMapped, self).__init__(*args, **kwargs)

    def make_command(self, filename):
        output = self.rebase_file(filename)
        output = self.replace_extension_with(".txt", output)
        command = ("bam validate --in {i} --verbose > {o}").format(i=filename,
                                                                   o=output)
        return (command)
