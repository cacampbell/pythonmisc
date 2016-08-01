#!/usr/bin/env python3
from sys import stderr

from PairedEndCommand import PairedEndCommand


class BBWrapper(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(BBWrapper, self).__init__(*args, **kwargs)
        self.set_default("reference", None)
        self.set_default("build", None)
        self.set_default("mode", "DNA")
        self.set_default("max_intron", "100k")
        self.set_default("pigz", False)

    def make_command(self, read):
        pass

    def format_commands(self):
        job_name = "{}".format(self.cluster_options["job_name"])
        in1 = ",".join(self.files)
        mates = [self.mate(x) for x in self.files]
        in2 = ",".join(mates)
        mapped = [self.rebase_file(x) for x in self.files]
        mapped = [self.replace_extension_with(".sam", x) for x in mapped]
        mapped = [self.replace_read_marker_with("_pe", x) for x in mapped]
        unmapped = [self.rebase_file(x) for x in self.files]
        unmapped = [self.replace_extension_with(".unmapped.sam", x) for x in unmapped]
        unmapped = [self.replace_read_marker_with("_pe", x) for x in unmapped]
        outm = ",".join(mapped)
        outu = ",".join(unmapped)
        command = ("bbwrap.sh in1={i1} in2={i2} outm={om} outu={ou} "
                   "threads={t} slow k=12 -Xmx{xmx} "
                   "usejni=t").format(i1=in1,
                                      i2=in2,
                                      om=outm,
                                      ou=outu,
                                      xmx=self.get_mem(fraction=0.95),
                                      t=self.get_threads())

        if self.mode.upper().strip() == "RNA":
            command = command + (" intronlen=10 ambig=random"
                                 " xstag=firststrand maxindel={}").format(
                self.max_intron)
        else:
            command += (" maxindel={}").format(self.max_intron)

        if self.pigz:
            command += (" pigz=t unpigz=t")
        else:
            command += (" pigz=f unpigz=f")

        if self.reference:
            command += (" ref={ref} nodisk").format(ref=self.reference)
        elif self.build:
            command += (" build={build}").format(build=self.build)

        self.commands[job_name] = command

        if self.verbose:
            print(command, file=stderr)
