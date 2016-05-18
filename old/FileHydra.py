#!/usr/bin/env python
from __future__ import print_function

import sys
from queue import Queue


class FileHydra:
    def __init__(self, files, flags):
        assert(files and flags)
        assert(type(files) is str)
        assert(type(flags) is str)
        assert(len(files.split()) == len(files.split()))
        self.files = files
        self.flags = flags

    def __enter__(self):
        self.file_handles = Queue()
        for filename, flags in zip(self.files.split(), self.flags.split()):
            try:
                self.file_handles.put(open(filename, flags))
            except (OSError, ValueError) as error:
                print("Error opening: {}, {}".format(filename, error),
                      file=sys.stderr)
        return self.file_handles

    def __exit__(self, type_, value_, traceback_):
        while not self.file_handles.empty():
            try:
                file_handle = self.file_handles.get()
                file_handle.close()
            except Exception as error:
                print("Error closing: {}".format(error),
                      file=sys.stderr)
