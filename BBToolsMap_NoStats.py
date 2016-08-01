#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class BBMapperNoStats(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(BBMapperNoStats, self).__init__(*args, **kwargs)
        self.set_default("reference", None)
        self.set_default("mode", "DNA")
        self.set_default("max_intron", "100k")
        self.set_default("pigz", False)
        self.set_default("read_groups", False)
        # Set read_regex here if necessary

    def make_command(self, read):
        mate = self.mate(read)
        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension_with(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)
        unmap_sam = self.replace_read_marker_with("_pe", read)
        unmap_sam = self.replace_extension_with(".unmapped.sam", unmap_sam)
        unmap_sam = self.rebase_file(unmap_sam)
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} "
                   "threads={t} slow k=12 -Xmx{xmx} "
                   "usejni=t").format(i1=read,
                                      i2=mate,
                                      om=map_sam,
                                      ou=unmap_sam,
                                      xmx=self.get_mem(),
                                      t=self.get_threads())

        if self.mode.upper().strip() == "RNA":
            command += (" maxindel={} xstag=firststrand "
                        "intronlen=10 ambig=random").format(self.max_intron)
        else:
            command += (" maxindel={}").format(self.max_intron)

        if self.pigz:
            command += (" pigz=t unpigz=t")
        else:
            command += (" pigz=f unpigz=f")

        if self.reference:
            command += (" ref={} nodisk").format(self.reference)

        if self.read_groups:
            command += (" rglb={rglb} rgpl={rgpl} "
                        "rgpu={rgpu} rgsm={rgsm}").format(
                **self.read_groups(read)
            )

        return (command)
