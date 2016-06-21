#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class BBWrapper(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(self, BBWrapper).__init__(*args, **kwargs)
        self.set_default("reference", "reference.fa")

    def make_command(self, read):
        pass

    def format_commands(self):
        job_name = "{}{}".format(self.cluster_options["job_name"])
        in1 = [",".join(x) for x in self.files]
        mates = [self.mate(x) for x in self.files]
        in2 = [",".join(x) for x in mates]
        mapped = [self.rebase_file(x) for x in self.files]
        mapped = [self.replace_extension_with(".sam", x) for x in mapped]
        mapped = [self.replace_read_marker_with("_pe", x) for x in mapped]
        unmapped = [self.rebase_file(x) for x in self.files]
        unmapped = [self.replace_extension_with(".sam", x) for x in unmapped]
        unmapped = [self.replace_read_marker_with("_pe", x) for x in unmapped]
        outm = [",".join(x) for x in mapped]
        outu = [",".join(x) for x in unmapped]
        command = ("bbwrap.sh in1={i1} in2={i2} outm={om} outu={ou} nodisk "
                   "threads={t} ref={r} slow k=12 -Xmx{xmx} monitor=600,0.01 "
                   "usejni=t").format(i1=in1,
                                      i2=in2,
                                      om=outm,
                                      ou=outu,
                                      xmx=self.get_mem(),
                                      t=self.get_threads(),
                                      r=self.reference)
        self.commands[job_name] = command

