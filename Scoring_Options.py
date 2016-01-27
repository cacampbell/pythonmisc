#!/usr/bin/env python
import sys
import unittest


class Scoring_Options:
    def __init__(self, A_matchScore="1", B_mmPenalty="4", O_gapOpenPen="6,6",
                 E_gapExtPen="1,1", L_clipPen="5,5", U_unpairPen="17"):
        """
        -A INT        score for a sequence match, which scales options
                      -TdBOELU unless overridden [1]
        -B INT        penalty for a mismatch [4]
        -O INT[,INT]  gap open penalties for deletions and insertions [6,6]
        -E INT[,INT]  gap extension penalty; a gap of size k cost '{-O} +
                      {-E}*k' [1,1]
        -L INT[,INT]  penalty for 5'- and 3'-end clipping [5,5]
        -U INT        penalty for an unpaired read pair [17]}}
        """
        self.A_matchScore = A_matchScore
        self.B_mmPenalty = B_mmPenalty
        self.O_gapOpenPen = O_gapOpenPen
        self.E_gapExtPen = E_gapExtPen
        self.L_clipPen = L_clipPen
        self.U_unpairPen = U_unpairPen

    def validate(self, verbose):

        if verbose:
            sys.stdout.write("Validating scoring options...\n")

        assert(int(self.A_matchScore) >= 0 and int(self.A_matchScore) <= 50)
        assert(int(self.B_mmPenalty) >= 0 and int(self.B_mmPenalty) <= 50)
        gap_open = self.O_gapOpenPen.split(",")
        gap_ext = self.E_gapExtPen.split(",")
        clip_pen = self.L_clipPen.split(",")
        assert(len(gap_open) == 2)
        assert(len(gap_ext) == 2)
        assert(len(clip_pen) == 2)
        assert(int(gap_open[0]) >= 0 and int(gap_open[0]) <= 50)
        assert(int(gap_open[1]) >= 0 and int(gap_open[1]) <= 50)
        assert(int(gap_ext[0]) >= 0 and int(gap_ext[0]) <= 50)
        assert(int(gap_ext[1]) >= 0 and int(gap_ext[1]) <= 50)
        assert(int(clip_pen[0]) >= 0 and int(clip_pen[0]) <= 50)
        assert(int(clip_pen[1]) >= 0 and int(clip_pen[1]) <= 50)


class Test_Scoring_Options(unittest.TestCase):
    pass
