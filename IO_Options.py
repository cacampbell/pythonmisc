#!/usr/bin/env python
import sys
import unittest


class IO_Options:
    def __init__(self, p=False, R=None, v="2", T="30", h="5", a=False, C=False,
                 V=False, Y=False, M=False):
        """
    -p            first query file consists of interleaved paired-end sequences
    -R STR        read group header line such as '@RG\\tID:foo\\tSM:bar' [null]
    -v INT        verbose level: 1=error, 2=warning, 3=message, 4+=debugging [3]
    -T INT        minimum score to output [30]
    -h INT        if there are <INT hits with score >80% of the max score,
                  output all in XA [5]
    -a            output all alignments for SE or unpaired PE
    -C            append FASTA/FASTQ comment to SAM output
    -V            output the reference FASTA header in the XR tag
    -Y            use soft clipping for supplementary alignments
    -M            mark shorter split hits as secondary
        """
        self.p = p
        self.R = R
        self.v = v
        self.T = T
        self.h = h
        self.a = a
        self.C = C
        self.V = V
        self.Y = Y
        self.M = M

    def validate(self, verbose):

        if verbose:
            sys.stdout.write("Validating io options...\n")

        # P can be anything, but might lead to unexpected results in coercion
        # from the passed value to True or False
        if self.R is not None:
            assert(isinstance(self.R, str))
        assert(int(self.R) >= 0 and int(self.R) <= 3)
        assert(int(self.T) >= 1 and int(self.T) <= 100)
        # h can be anything, coercion to integer will produce unexpected
        # results for strings of non-numerical characters
        assert(isinstance(self.a, bool))
        assert(isinstance(self.C, bool))
        assert(isinstance(self.V, bool))
        assert(isinstance(self.Y, bool))
        assert(isinstance(self.M, bool))


class Test_IO_Options(unittest.TestCase):
    def test_validate(self):
        pass
