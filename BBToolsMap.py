#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class BBMapper(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(BBMapper, self).__init__(*args, **kwargs)
        self.set_default("reference", None)
        self.set_default("build", None)
        self.set_default("mode", "DNA")
        self.set_default("max_intron", "100k")
        self.set_default("pigz", False)
        self.set_default("read_group", False)
        # Set read_regex here if necessary

    def make_command(self, read):
        # Mate File
        mate = self.mate(read)

        # Mapped Sam file
        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension_with(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)

        # Unmapped Sam file
        unmap_sam = self.replace_read_marker_with("_pe", read)
        unmap_sam = self.replace_extension_with(".unmapped.sam", unmap_sam)
        unmap_sam = self.rebase_file(unmap_sam)

        # Scaffold statistics file
        scaf = self.replace_read_marker_with("_pe", read)
        scaf = self.replace_extension_with(".scafstats.txt", scaf)
        scaf = self.rebase_file(scaf)

        # Statistics file
        stats = self.replace_read_marker_with("_pe", read)
        stats = self.replace_extension_with(".stats.txt", stats)
        stats = self.rebase_file(stats)

        # Coverage Statistics file
        cov = self.replace_read_marker_with("_pe", read)
        cov = self.replace_extension_with(".covstats.txt", cov)
        cov = self.rebase_file(cov)

        # Full Command
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} "
                   "threads={t} slow k=12 -Xmx{xmx} usejni=t "
                   "scafstats={scaf} statsfile={stats} covstats={cov}").format(
            i1=read,
            i2=mate,
            om=map_sam,
            ou=unmap_sam,
            xmx=self.get_mem(),
            t=self.get_threads(),
            scaf=scaf,
            stats=stats,
            covstats=cov
        )

        if self.mode.upper().strip() == "RNA":
            command += (" intronlen=10 ambig=random "
                        "xstag=firststrand maxindel={}").format(self.max_intron)
        else:
            command += (" maxindel={}").format(self.max_intron)

        if self.pigz:
            command += (" pigz=t unpigz=t")
        else:
            command += (" pigz=f unpigz=f")

        if self.reference:
            command += (" ref={} nodisk").format(self.reference)
        elif self.build:
            command += (" build={build}")

        if self.read_groups:
            command += (" rglb={rglb} rgpl={rgpl}"
                        " rgpu={rgpu} rgsm={rgsm}").format(
                **self.read_groups(read)
            )

        return (command)
