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
        self.set_default("read_groups", False)
        self.set_default("use_modulo", False)
        self.set_default("stats", False)
        self.set_default("speed", "normal")
        self.set_default("num_reads", "-1")
        # According to the BBMap documentation, increasing k and minhits
        # increases speed. There is also a built in speed flag that can be
        # passed directly to the program, which 'slightly reduces sentitivity'
        # with increasing speed.
        self.__speeds = {
            'vfast': ' fast k=15 minhits=3',
            'fast': ' fast k=14 minhits=2',
            'normal': ' k=13 minhits=1',
            'slow': ' slow k=12 minhits=1',
            'vslow': ' vslow k=11 minhits=1'
        }

    def make_command(self, read):
        mate = self.mate(read)
        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension_with(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)
        unmap_sam = self.replace_read_marker_with("_pe", read)
        unmap_sam = self.replace_extension_with(".unmapped.sam", unmap_sam)
        unmap_sam = self.rebase_file(unmap_sam)
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} "
                   "threads={t} -Xmx{xmx} "
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

        if self.speed in self.__speeds.keys():
            command += self.__speeds[self.speed]
        else:
            command += self.__speeds['normal']

        if self.pigz:
            command += (" pigz=t unpigz=t")
        else:
            command += (" pigz=f unpigz=f")

        if self.reference:
            command += (" ref={ref} nodisk").format(ref=self.reference)
        elif self.build:
            command += (" build={build}").format(build=self.build)

        if self.use_modulo:
            command += (" usemodulo=t")

        if self.num_reads:
            command += (" reads={}").format(self.num_reads)

        if self.stats:
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

            command += (" scafstats={scaf} "
                        "statsfile={stats} "
                        "covstats={cov}").format(scaf=scaf,
                                                 stats=stats,
                                                 cov=cov)

        if self.read_groups:
            command += (" rglb={rglb} rgpl={rgpl}"
                        " rgpu={rgpu} rgsm={rgsm}").format(
                **self.get_read_groups(read)
            )

        return (command)
