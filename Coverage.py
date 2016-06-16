#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand


class Coverage(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(Coverage, self).__init__(*args, **kwargs)

    def make_command(self, mapped):
        # works with SAM or BAM
        normcov = self.rebase_file(mapped)
        normcov = self.replace_read_marker_with("_normcov", normcov)
        normcov = self.replace_extension_with(".txt", normcov)

        covstats = self.rebase_file(mapped)
        covstats = self.replace_read_marker_with("_covstats", covstats)
        covstats = self.replace_extension_with(".txt", covstats)

        hist = self.rebase_file(mapped)
        hist = self.replace_read_marker_with("_hist", hist)
        hist = self.replace_extension_with(".txt", hist)

        stats = self.rebase_file(mapped)
        stats = self.replace_read_marker_with("_stats", stats)
        stats = self.replace_extension_with(".txt", stats)

        bincov = self.rebase_file(mapped)
        bincov = self.replace_read_marker_with("_bincov", bincov)
        bincov = self.replace_extension_with(".txt", bincov)

        command = ("pileup.sh -Xmx{xmx} threads={t} in={i} normcov={normcov} "
                   "covstats={covstats} hist={hist} stats={stats} "
                   "bincov={bincov}").format(
            xmx=self.get_mem(fraction=0.95),
            t=self.get_threads(),
            i=mapped,
            normcov=normcov,
            covstats=covstats,
            hist=hist,
            stats=stats,
            bincov=bincov
        )

        return (command)
