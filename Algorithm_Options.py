#!/usr/bin/env python
import sys
import unittest


class Algorithm_Options:
    def __init__(self, k_minSeedLength="19", w_bandWidth="100",
                 d_zDropoff="100", r_seedSplitRatio="1.5", c_maxOcc="500",
                 P=False):
        """
        -k INT        minimum seed length [19]
        -w INT        band width for banded alignment [100]
        -d INT        off-diagonal X-dropoff [100]
        -r FLOAT      look for internal seeds inside a seed longer than {-k} *
                      FLOAT [1.5]
        -c INT        skip seeds with more than INT occurrences [500]
        -D FLOAT      drop chains shorter than FLOAT fraction of the longest
                      overlapping chain [0.50]
        -P            skip pairing; mate rescue performed unless -S also in use
        """
        self.k_minSeedLength = k_minSeedLength
        self.w_bandWidth = w_bandWidth
        self.d_zDropoff = d_zDropoff
        self.r_seedSplitRatio = r_seedSplitRatio
        self.c_maxOcc = c_maxOcc
        self.P = P

    def validate(self, verbose):

        if verbose:
            sys.stdout.write("Validating algorithm options...\n")

        assert(int(self.k_minSeedLength) >= 1 and int(self.k_minSeedLen) <= 50)
        assert(int(self.w_bandWidth) >= 1 and int(self.w_bandWidth) <= 500)
        assert(int(self.d_zDropoff) >= 1 and int(self.d_zDropoff) <= 500)
        assert(int(self.maxOcc) >= 1 and int(self.maxOcc) <= 10000)
        assert(isinstance(self.P, bool))


class Test_Align_Param(unittest.TestCase):
    def test_validate(self):
        pass
