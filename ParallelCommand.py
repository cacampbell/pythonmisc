#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import re
from subprocess import call
import errno
from module_loader import module
from clusterlib.scheduler import queued_or_running_jobs
from clusterlib.scheduler import submit
import unittest
from abc import abstractmethod
from abc import ABCMeta


class ParallelCommand:
    """
    ParallelCommand
    A class to gather files, write commands, and dispatch those commands to the
    slurm manager. Finds all files at the input root, creates directories, and
    dispatches commands. The actual commands are provided by derived classes
    for various jobs.
    """
    __metaclass__ = ABCMeta  # Make this class virtual

    @staticmethod
    def rebase(filename, src_root, dest_root):
        #  create relative output directory (rebase from src_root
        #  to dest_root)
        return os.path.join(dest_root, os.path.relpath(filename,
                                                       start=src_root))

    def output_file(self, filename):
        #  create the output file for the given filename
        return ParallelCommand.rebase(filename,
                                      self.input_root,
                                      self.output_root)

    @staticmethod
    def mkdir_p(path):
        """
        Emulate mkdir -p behavior from unix systems -- attempts to make
        a directory, prints a message if that directory already exists
        :param path: the path to make
        :return:
        """
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                print("{} already exists".format(path),
                      file=sys.stderr)
            else:
                raise exc

    def __init__(self, input_=os.getcwd(), output_=os.getcwd()):
        self.input_root = input_
        self.output_root = output_
        self.slurm_options = {
            'mem': '200G',
            'tasks': '1',
            'cpus': '24',
            'job-name': 'Dispatch',
            'time': '0',
            'mail-user': 'example@example.com',
            'mail-type': 'END,FAIL',
            'partition': 'bigmemm'
        }
        self.modules = ['']
        self.job_prefix = ''
        self.input_suffix = ''
        self.dry_run = True
        self.verbose = True
        self.exclusions_directory = None
        self.exclusions = None
        self.__files = []
        self.__commands = {}
        self.__scripts = {}
        self.__exclusions = []

    def get_threads(self):
        """
        Calculates the number of available threads, assuming 4 per cpu
        Allows 2 free cores to be used by non-worker processes
        :return: str: number of available worker threads
        """
        return str(int(self.slurm_options['cpus']) * 4 - 2)

    def get_mem(self, fraction=1):
        """
        Get fraction of available memory
        :param fraction: fraction of memory to get
        :return: str: available memory + unit of measure
        """
        assert (float(fraction) <= 1.0)
        mem_int = float(self.slurm_options['mem'][:-1])  # All but last char
        mem_unit = self.slurm_options['mem'][-1]  # last char
        memory = float(mem_int) * float(fraction)  # fraction of avail mem
        memory = int(memory)  # Must be int for slurm version on farm
        return "{}{}".format(memory, mem_unit)  # [\d][U]

    def dispatch(self):
        """
        Dispatch scripts to the slurm manager
        :return:
        """
        scheduled_jobs = set(queued_or_running_jobs())  # current jobs
        for job_name, script in self.__scripts.items():  # each script
            if job_name not in scheduled_jobs:  # not already running
                if self.verbose:
                    print("Dispatching {}...".format(job_name), file=sys.stderr)
                if not self.dry_run:  # Call the script
                    call(script, shell=True)
            else:
                print("{} already running, skipping...".format(job_name),
                      file=sys.stderr)

    def scripts(self):
        """
        Write scripts to be dispatched to the slurm manager
        Wraps each earlier crafted command into a job for slurm, using the
        slurm API
        :return:
        """
        #TODO: Refactor
        for job_name, command in self.__commands.items():  # for each cmd
            script = submit(command,
                            job_name=job_name or self.slurm_options['job_name'],
                            time=self.slurm_options['time'],
                            memory=self.slurm_options['mem'],
                            backend='slurm',
                            shell_script='#!/usr/bin/env bash')
            script += " --partition={}".format(self.slurm_options['partition'])
            script += " --ntasks={}".format(self.slurm_options['tasks'])
            script += " --cpus-per-task={}".format(self.slurm_options['cpus'])
            script += " --mail-user={}".format(self.slurm_options['mail-user'])
            script += " --mail-type={}".format(self.slurm_options['mail-type'])
            self.__scripts[job_name] = script  # Script wraps cmd for slurm

            if self.verbose:
                print(script)

    @abstractmethod
    def make_command(self, read):
        """
        Abstract method, must be overridden
        The command for each read (each file matching suffix), can be generated
        in this method.
        :param read:
        :return:
        """
        pass

    def commands(self):
        """
        Generate commands for each file gathered
        :return:
        """
        for read in self.__files:  # for each file
            job_name = "{}{}".format(self.job_prefix, os.path.basename(read))
            command = self.make_command(read)  # derived class makes command
            assert (type(command) is str)  # at least, it has to be a str
            self.__commands[job_name] = command

            if self.verbose:
                print(command)

    def __exclude_regex_matches_list(self, exclusions):
        for regex in exclusions:
                for filename in list(self.__files):  # *copy* of the file list
                    if re.search(regex, filename) or regex in filename:
                        if self.verbose:
                            print("Match: {} {}".format(regex, filename))

                        self.__files.remove(filename)

                        if self.verbose:
                            print("Removed {}, matching {}".format(filename,
                                                                    regex))

    def __exclude_regex_matches_file(self, exclusions):
        try:
            with open(exclusions, "rb") as fh:  # try to open it
                for regex in fh.readlines():  # for each line in it
                    for filename in list(self.__files):
                        #  If it matches a filename in __files, remove
                        if re.search(regex, filename) or regex in filename:
                            self.__files.remove(filename)

                        if self.verbose:
                            print("Removed {}, matching {}".format(
                                  filename, regex))

        except (OSError, IOError) as error:  # ... but couldn't open it
            print("{} occurred while trying to read {}".format(
                error, exclusions), file=sys.stderr)
            print("No exclusions removed...", file=sys.stderr)

    def __exclude_regex_matches_single(self, exclusions):
        for filename in list(self.__files):
            # So, it's a regex, search __files and remove if match
            if re.search(exclusions, filename) or exclusions in filename:
                self.__files.remove(exclusions)

                if self.verbose:
                    print("Removed {}, matching {}".format(filename,
                                                            exclusions))

    def exclude_regex_matches(self, exclusions):
        """
        Remove all files from the list of files that match "exclusions".
        "exclutions" should be either a single string regex / substring, a list
        of regexes / substrings, or a file containing a list of regexes /
        substrings (one per line). This method attempts to differentiate
        between these options by checking the type, then checking if the
        string is a file on the system. If it is a file, then this method will
        attempt to open it and read the lines into a list for usage as regexes /
        substrings.
        :exclusions: str|list: regex/substring, list of regexes/substrings, file
        """
        if type(exclusions) is list:
            self.__exclude_regex_matches_list(exclusions)

        elif type(exclusions) is str:  # inclusions is either file or regex
            if os.path.isfile(exclusions):  # it's a file
                self.__exclude_regex_matches_file(exclusions)
            else:  # Not a file on the system
                self.__exclude_regex_matches_single(exclusions)

        else:  # Didn't get expected types, print message and continue
            print("Exclusions not str or list, no exclusions removed...",
                  file=sys.stderr)

    def exclude_files_from(self, previous_output_root):
        """
        Find files that are below previous_output_path, strip them down to
        the basename of those files (the name of each without extensions), then
        use this list to remove exclusions from the gathered file list. This
        method is given a directory that contains previous output and prevents
        those files from being run again in the current run. Written in case of
        needing to requeue large sets of jobs due to no space left on the
        previously used device.
        :param previous_output_root: str: a directory path (previous output)
        :return:
        """
        exclusions = []

        for root, dirs, files in os.walk(previous_output_root):
            for filename in files:
                f = os.path.splitext(filename)[0]

                if "_pe" in f:
                    exclusions += [f.replace("_pe", self.read_marker)]
                    exclusions += [f.replace("_pe", self.mate_marker)]
                else:
                    exclusions += [f]

        self.__exclude_regex_matches_list(list(set(exclusions)))

    def get_files(self):
        """
        Gather files below the input_root such that those files end with
        input_suffix and contain read_marker within their name. For example,
        input_suffix might be ".fq.gz" and read_marker might be "R1". This
        method would thus gather all reads that match ^.*R1.*\.fq\.gz$
        :return:
        """
        for root, _, files in os.walk(self.input_root):
            for filename in files:
                if re.search(self.input_suffix, filename):  #TODO: refactor
                    abs_path = os.path.join(root, filename)
                    self.__files += [abs_path]

                    if self.verbose:
                        print(abs_path, file=sys.stderr)

    def load_modules(self):
        """
        Load environment modules using environment module system
        :return:
        """
        try:
            args = ['load']  # first argument is always 'load'
            args.extend(self.modules)  # add specified modules to arguments
            module(args)  # call module system, using arguments ['load', '...']
        except (OSError, ValueError) as err:
            print("Could not load module(s):{0:s}, {0:s}".format(self.modules,
                                                                 err))

    def make_directories(self):
        """
        Make the relative output directories that are necessary to preserve
        output directory structure at the specified output root. All directories
        below input_root will be created below output root
        :return:
        """
        directories = [x[0] for x in os.walk(self.input_root)]  # all dirs
        output_directories = [self.rebase(x, self.input_root, self.output_root)
                              for x in directories]  # rebase each dir

        for directory in output_directories:
            if self.verbose:
                print("Attempting to make: {}".format(directory))
            if not self.dry_run:
                self.mkdir_p(directory)  # Attempt safe creation of each dir

    def run(self):
        if self.verbose:
            print('Loading environment modules...', file=sys.stderr)
        self.load_modules()

        if self.verbose:
            print('Gathering input files...', file=sys.stderr)
        self.get_files()

        if self.verbose:
            print('Removing exclusions...')
        if self.exclusions_directory:
            self.exclude_files_from(self.exclusions_directory)
        if self.exclusions:
            self.exclude_regex_matches(self.exclusions)

        if self.verbose:
            print("Making output directories...", file=sys.stderr)
        self.make_directories()

        if self.verbose:
            print('Writing commands...', file=sys.stderr)
        self.commands()

        if self.verbose:
            print('Writing scripts...', file=sys.stderr)
        self.scripts()

        if self.verbose:
            print('Dispatching scripts to slurm...', file=sys.stderr)
        self.dispatch()


class TestParallelCommand(unittest.TestCase):
    def setUp(self):
        pass

    def test_make_directories(self):
        pass

    def test_load_modules(self):
        pass

    def test_get_files(self):
        pass

    def scripts(self):
        pass

    def dispatch(self):
        pass

    def test_run(self):
        pass

    def tearDown(self):
        pass


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestParallelCommand)
    unittest.TextTestRunner(verbosity=3).run(suite)
