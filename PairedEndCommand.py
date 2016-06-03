#!/usr/bin/env python3
import unittest
from sys import stderr

from abc import ABCMeta
from abc import abstractmethod
from os import path
from os import walk
from os.path import basename
from re import search
from re import sub

from ParallelCommand import ParallelCommand

__all__ = ["PairedEndCommand"]

class PairedEndCommand(ParallelCommand):
    """
    Extension of ParallelCommand to run parallel commands with paired end
    sequencing files, mainly for Illumina data
    Makes small changes to the file gathering and init methods that every
    Paired job needs in order to run
    """
    __metaclass__ = ABCMeta  # Still requires overwrite for make_command

    def __init__(self, *args, **kwargs):
        """
        Initialize this class using arguments passed to it

        Expected positional args:
            None

        Expcected kwargs:
            Unique to PairedEndCommand
                :param: read_regex: str: regex for paired end files 1/2
                :param: reference: str: optional: the reference genome

        :param: input_root: str: the input root for this series of commands
        :param: output_root: str: the output root for this series of commands
        :param: input_regex: str: regex specifying all input files
        :param: extension: str: regex for extension on files
        :param: exclusions: str: regex, comma separated list of regex, or python
        list that specifies which files are to be excluded from the given run
        :param: exlcusions_path: str: a directory conaining files with basenames
        that should be excluded from the given run
        :param: dry_run: bool: Toggles whether or not commands are actually run
        :param: verbose: bool: Toggles print statements throughout
        :param: cluster_options: dict: dictionary of cluster options
            memory - The memory to be allocated to this job
            nodes - The nodes to be allocated
            cpus - The cpus **per node** to request
            partition -  The queue name or partition name for the submitted job
            job_name - The name of the job
            depends_on - The dependencies (as comma separated list of job numbers)
            email_address -  The email address to use for notifications
            email_options - Email options: START|BEGIN,END|FINISH,FAIL|ABORT
            time - time to request from the scheduler
            bash -  The bash shebang line to use in the script

        Any other keyword arguments will be added as an attribute with that name
        to the instance of this class. So, if additional parameters are needed
        for formatting commands or any other overriden methods, then they
        can be specified as a keyword agument to init for convenience
        """
        super(PairedEndCommand, self).__init__(*args, **kwargs)
        self.set_default("read_regex", "_R1\.fq.*")


    def mate(self, read):
        """
        Return the filename of the mate for this read
        :param: read: str: the read filename
        """
        try:
            read_match = search(self.read_regex, read).group(0)
            mate_match = sub("1", "2", read_match)
            return (sub(read_match, mate_match, read))
        except AttributeError as err:
            print("Could not find and replace using read_regex: {}".format(err),
                  file=stderr)

    def __replace_regex(self, regex, replacement, string):
        try:
            match = search(regex, string).group(0)
            return (sub(match, replacement, string))
        except AttributeError as err:
            print("Could not find and replace using read_regex: {}".format(err),
                  file=stderr)

    def replace_read_marker_with(self, replacement, read):
        return self.__replace_regex(self.read_regex, replacement, read)

    def replace_extension(self, extension, read):
        if self.extension:
            try:
                return (read.replace(self.extension, extension))
            except Exception as err:
                print("Cannot find and replace by extension: {}".format(err),
                      file=stderr)
                raise (err)
        try:
            return (read.rsplit(".", 1)[0] + extension)
        except Exception as err:
            print("Could not automatically replace extension: {}".format(err),
                  file=stderr)
            raise (err)

    def remove_files_below(self, root):
        if type(root) is list:
            for directory in root:
                self.remove_files_below(directory)

        if "," in root:
            for directory in root.split(","):
                self.remove_files_below(directory)

        exclusions = []

        if path.isdir(root):
            if self.verbose:
                print("Removing files form {}".format(root), file=stderr)

            for root, dir, files in walk(root):
                for filename in files:
                    base = basename(filename)
                    base_no_ext = path.splitext(base)[0]
                    exclusions += [base_no_ext]
                    possible_input = self.__replace_regex("_pe", "_R1",
                                                          base_no_ext)
                    exclusions += [possible_input]
                    possible_input2 = self.__replace_regex("_pe", "_1",
                                                           base_no_ext)
                    exclusions += [possible_input2]

        for regex in list(set(exclusions)):
            self.remove_regex_from_input(regex)

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
                    if self.extension is not None:
                        if search(self.extension, filename):
                            abs_path = path.join(root, filename)
                            self.files += [abs_path]

                            if self.verbose:
                                print(abs_path, file=stderr)
                    else:
                        abs_path = path.join(root, filename)
                        self.files += [abs_path]

                        if self.verbose:
                            print(abs_path, file=stderr)

    @abstractmethod
    def make_command(self, filename):
        pass


class TestPairedEndCommand(unittest.TestCase):
    def setUp(self):
        pass

    def test_mate(self):
        pass

    def test_replace_read_marker_with(self):
        pass

    def test_replace_extension(self):
        pass

    def test_get_files(self):
        pass

    def tearDown(self):
        pass


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPairedEndCommand)
    unittest.TextTestRunner(verbosity=3).run(suite)