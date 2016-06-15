#!/usr/bin/env bash
from PairedEndCommand import PairedEndCommand


class CleanSort(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(CleanSort, self).__init__(*args, **kwargs)

    def make_command(self, read):
        mate = self.mate(read)
        # Clean and Sort output from mapping
        # CleanSAM + Fixmate
        # MergeBAMAlignment
        # repair.sh, samtools fixmate, samtools sort
        pass