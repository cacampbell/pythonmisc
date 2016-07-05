#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class BBMapper(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(BBMapper, self).__init__(*args, **kwargs)
        self.set_default("reference", "reference.fa")
        self.set_default("mode", "DNA")
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

        # Coverage Statistics
        covstat = self.replace_read_marker_with("_covstats", read)
        covstat = self.replace_extension_with(".txt", covstat)
        covstat = self.rebase_file(covstat)

        # Coverage Hist
        covhist = self.replace_read_marker_with("_covhist", read)
        covhist = self.replace_extension_with(".txt", covhist)
        covhist = self.rebase_file(covhist)

        # Base Coverage
        basecov = self.replace_read_marker_with("_basecov", read)
        basecov = self.replace_extension_with(".txt", basecov)
        basecov = self.rebase_file(basecov)

        # Bin Coverage
        bincov = self.replace_read_marker_with("_bincov", read)
        bincov = self.replace_extension_with(".txt", bincov)
        bincov = self.rebase_file(bincov)

        # Full Command
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} nodisk "
                   "covstats={covstat} covhist={covhist} threads={t} ref={r} "
                   "slow k=12 -Xmx{xmx} basecov={basecov} usejni=t"
                   " bincov={bincov}").format(
            i1=read,
            i2=mate,
            om=map_sam,
            ou=unmap_sam,
            covstat=covstat,
            covhist=covhist,
            basecov=basecov,
            bincov=bincov,
            xmx=self.get_mem(),
            t=self.get_threads(),
            r=self.reference)

        if self.mode.upper().strip() == "RNA":
            command += (" intronlen=10 ambig=random "
                        "xstag=firststrand maxindel=50000")

        return (command)
