#!/usr/bin/env python3
import bz2
import gzip
import unittest
import zipfile
from sys import stderr

from abc import abstractmethod


class CompressedFile(object):
    magic = None
    file_type = None
    mime_type = None
    proper_extension = None

    def __init__(self, h):
        self.handle = h
        self.accessor = None

    @abstractmethod
    def open(self):
        raise (RuntimeError("Called non-overidden abstract method"))

    @abstractmethod
    def close(self):
        raise (RuntimeError("Called non-overidden abstract method"))

    def __enter__(self):
        return self.open()

    def __exit__(self, exception_type, exception_value, traceback):
        try:
            return self.close()
        except Exception as err:
            print(exception_type, exception_value, traceback)
            raise (err)

    @classmethod
    def is_magic(cls, data):
        assert (type(data) == type(cls.magic))
        return data.startswith(cls.magic)


class ZIPFile(CompressedFile):
    magic = b'\x50\x4b\x03\x04'
    file_type = 'zip'
    mime_type = 'compressed/zip'
    proper_extension = "zip"

    def open(self):
        self.accessor = zipfile.ZipFile(self.handle, 'r')
        return self.accessor

    def close(self):
        try:
            return self.accessor.close()
        except ValueError:
            pass  # ValueError due to closed file handle


class BZ2File(CompressedFile):
    magic = b'\x42\x5a\x68'
    file_type = 'bz2'
    mime_type = 'compressed/bz2'
    proper_extension = 'bz2'

    def open(self):
        self.accessor = bz2.BZ2File(self.handle, 'r')
        return self.accessor

    def close(self):
        try:
            return self.accessor.close()
        except ValueError:
            pass


class GZFile(CompressedFile):
    magic = b'\x1f\x8b\x08'
    file_type = 'gz'
    mime_type = 'compressed/gz'
    proper_extension = 'gz'

    def open(self):
        self.accessor = gzip.GzipFile(self.handle, 'r')
        return self.accessor

    def close(self):
        try:
            return self.accessor.close()
        except ValueError:
            pass


def get_compressed(filename):
    with open(filename, 'rb') as f:
        start_of_file = f.read(1024)

        for cls in (ZIPFile, BZ2File, GZFile):
            if cls.is_magic(start_of_file):
                return cls(filename)

        return None


class TestCompression(unittest.TestCase):
    def setUp(self):
        def test(filename):
            try:
                with get_compressed(filename) as handle:
                    print("Context Managed by class {}".format(type(handle)))
            except TypeError as err:
                print("Not a supported zip filetype", file=stderr)

        self.tester = test

    def test_zip(self):
        self.tester('testdata/test.zip')

    def test_bz2(self):
        pass

    def test_gz(self):
        pass


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCompression)
    unittest.TextTestRunner(verbosity=3).run(suite)
