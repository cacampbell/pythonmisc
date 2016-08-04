#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class ValidateMapped(PairedEndCommand):
    """
    Validate SAM or BAM files
    """

    def __init__(self, *args, **kwargs):
        super(ValidateMapped, self).__init__(*args, **kwargs)
        self.input_regex=".*"
        self.read_regex=".*"
        self.extension=".[s|b]am"
        self.set_default("picard", "~/.prog/picard-tools-2.5.0/picard.jar")
        self.set_default("~/tmp")
        
    def make_command(self, filename):
        output = self.rebase_file(filename)
        output = self.replace_extension_with(".txt", output)
        command = ("java -Xms{xms} -Xmx{xmx} -Djava.io.tmpdir={tmp} -jar "
                   "{picard} ValidateSamFile I={input} OUTPUT={output} "
                   "MODE=SUMMARY IGNORE_WARNINGS").format(
            xms=self.get_mem(0.95),
            xmx=self.get_mem(0.98),
            tmp=self.tmp_dir,
            picard=self.picard,
            input=filename,
            output=output
        )

        return (command)
