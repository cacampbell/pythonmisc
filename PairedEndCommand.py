#!/usr/bin/env python3
from sys import stderr

from abc import ABCMeta
from abc import abstractmethod
from os import path
from os import walk
from re import search
from re import sub

from ParallelCommand import ParallelCommand


class PairedEndCommand(ParallelCommand):
    """
    Extension of ParallelCommand to run parallel commands with paired end
    sequencing files
    Makes small changes to the file gathering and init methods that every
    Paired job needs in order to run
    """
    __metaclass__ = ABCMeta

    def __init__(self, input_root, output_root):
        self.read_regex = ".*_R1\.fq.*"  # All input reads (file 1 of 2) match
        super(self, PairedEndCommand).__init__(input_root=input_root,
                                               output_root=output_root)

    def mate(self, read):
        """
        Return the filename of the mate for this read
        :param: read: str: the read filename
        """
        read_match = search(self.read_regex, read).group(1)
        mate_match = sub("1", "2", read_match)
        return (sub(read_match, mate_match, read))

    def get_files(self):
        """
            Gather all files that match the input_regex that are below the input
            directory
            :return:
        """
        for root, _, files in walk(self.input_root):
            for filename in files:  # for all files
                # Match input_regex and read_regex in the files found
                if (search(self.input_regex, filename) and
                        search(self.read_regex, filename)):
                    abs_path = path.join(root, filename)
                    self.__files += [abs_path]

                    if self.verbose:
                        print(abs_path, file=stderr)

    @abstractmethod
    def make_command(self, filename):
        pass
