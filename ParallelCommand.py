#!/usr/bin/env python3
import errno
import unittest
from sys import stderr

from abc import ABCMeta
from abc import abstractmethod
from os import getcwd
from os import makedirs
from os import path
from os import walk
from os.path import isdir
from re import search

from cluster_commands import existing_jobs
from cluster_commands import submit_job
from module_loader import module

__all__ = ["ParallelCommand"]


class ParallelCommand:
    """
    ParallelCommand
    Encapsulates the biolerplate required to run parallel identical jobs on
    groups of files using a cluster backend. This includes gathering files below
    a root directory, exlcuding undesired files, making a directory structure
    at the output root, formatting format_commands for each file, writing a bash
    script as a string for each format_commands, and finally, dispatching each
    script to the appropriate cluster backend
    """
    # Make this class virtual, since it requires make_command at the least to
    # be overidden. Different scenarios will overwrite different methods.
    __metaclass__ = ABCMeta

    @staticmethod
    def rebase_directory(filename, src_root, dest_root):
        """
        "Rebases" a filename by substituting the src_root for dest_root
        as the absolute path (preserves relative directory structure)
        """
        return path.join(dest_root, path.relpath(filename,
                                                 start=src_root))

    def rebase_file(self, filename):
        """
        "Rebases" a file using the instance input root and output root
        """
        return ParallelCommand.rebase_directory(filename,
                                                self.input_root,
                                                self.output_root)

    def __init__(self, input_root=getcwd(), output_root=getcwd(),
                 input_regex=".*"):
        """
        Initialize with an input root and an output root
        """
        self.input_root = input_root
        self.output_root = output_root
        self.input_regex = input_regex
        self.modules = ''
        self.extension = None
        self.exclusions = None
        self.exclusions_paths = None
        self.dry_run = False
        self.verbose = False

        # """
        #   memory - The memory to be allocated to this job
        #   nodes - The nodes to be allocated
        #   cpus - The cpus **per node** to request
        #   partition -  The queue name or partition name for the submitted job
        #   job_name - The name of the job
        #   depends_on - The dependencies (as comma separated list of job numbers)
        #   email_address -  The email address to use for notifications
        #   email_options - Email options: START|BEGIN,END|FINISH,FAIL|ABORT
        #   time - time to request from the scheduler
        #   bash -  The bash shebang line to use in the script
        #   input - The input filename for the job
        #   output - The output filename for the job
        #   error - The error filename for the job
        # """
        self.cluster_options = {
            "memory": "2G",
            "nodes": "1",
            "cpus": "1",
            "partition": "normal",
            "job_name": "ParallelCommand_",
            "depends_on": "",
            "email_user": "",
            "email_options": "END,FAIL",
            "time": "0",
            "bash": "#!/usr/bin/env bash"
        }

        self.files = []
        self.commands = {}
        self.exclusions = []

    def get_threads(self):
        """
        Calculates the number of threads based on the specified number of cores
        :return: str: number of available worker threads
        """
        return str(int(self.cluster_options["cpus"]) - 1)

    def get_mem(self, fraction=1):
        """
        Get the available memory, subset by fraction
        :param fraction: fraction of memory to get
        :return: str: available memory + unit of measure
        """
        assert (float(fraction) <= 1.0)
        mem_int = float(self.cluster_options['memory'][:-1])  # All but last
        mem_unit = self.cluster_options['memory'][-1]  # last char
        memory = float(mem_int) * float(fraction)  # fraction of avail mem
        memory = int(memory)  # Must be int, partial units cause error
        return "{0:s}{0:s}".format(memory, mem_unit)  # [\d][c], memory + units

    def dispatch(self):
        """
        For each command in self.commands, submit that command to the cluster
        scheduler using the desired options from self.cluster_options
        """
        job_numbers = []

        for job_name, command in self.commands.items():
            # Full name is whole task job name + individual name
            full_job_name = self.cluster_options["job_name"] + job_name

            # If the job is not already running and actually submitting the job
            if full_job_name not in existing_jobs() and not self.dry_run:
                # Replace the job name for the cluster options
                opts = dict(self.cluster_options)
                opts["job_name"] = full_job_name

                # Capture the job number for the submitted job
                job_number = submit_job(command, **opts)
                job_numbers.append(job_number)

                if self.verbose:
                    print("Submitted job: {}".format(job_number), file=stderr)
            else:
                print("Job {} already running or dry_run set to True".format(
                    full_job_name
                ), file=stderr)

        return (job_numbers)

    @abstractmethod
    def make_command(self, filename):
        """
        Ovveride this method to format command for each file
        The command used is applied to each file and added to the list of
        format_commands by looping through the list of files
        The rebase_file method provides the output file for the given filename

        :param filename: str: the filename that is being wrapped by this command
        :return:
        """
        pass

    def format_commands(self):
        """
        Generate format_commands for each file gathered
        :return:
        """
        for file in self.files:  # for each file
            command = ""

            job_name = "{0:s}{0:s}".format(
                self.cluster_options["job_name"], path.basename(file))

            try:
                command = self.make_command(file)  # derived class command
            except Exception as ex:
                if self.verbose:
                    print("Command formatting failed: {}".format(ex),
                          file=stderr)

            assert (type(command) is str)  # at least, it has to be a str
            self.commands[job_name] = command

            if self.verbose:
                print(command, file=stderr)

    def __exclude_regex_matches_single(self, exclusions):
        for filename in list(self.files):
            # So, it's a regex, search files and remove if match
            if search(exclusions, filename):
                self.files.remove(filename)

                if self.verbose:
                    print("Removed {0:s}, matching {0:s}".format(filename,
                                                                 exclusions),
                          file=stderr)

    def exclude_regex_matches(self, exclusion):
        """
        Remove all files from the list of files that match "exclusions".
        "exclutions" should be either a single string regex / substring, a list
        of regexes / substrings, or a file containing a list of regexes /
        substrings (one per line), or a list of files containing regexes.

        This method attempts to differentiate between these options by checking
        the type, then checking if the string is a file on the system. If it is
        a file, then this method will attempt to open it and read the lines into
        a list for usage as regexes / substrings.
        :exclusions: str|list: regex/substring, list of regexes/substrings, file
        """
        if type(exclusion) is list:
            for ex in exclusion:
                self.exclude_regex_matches(ex)
        elif type(exclusion) is str:  # inclusions is either file or regex
            self.__exclude_regex_matches_single(exclusion)
        else:  # Didn't get expected types, print message and continue
            print("Exclusions not str or list, no exclusions removed...",
                  file=stderr)

    def exclude_files_from(self, dirs):
        """
        Method that excludes files that are below <dirs>. If dirs is a list (
        presumably, of directory names) then remove files below the listed
        directories. If dirs contains a comma, assume that this denotes a list,
        and split the string on comma, then exclude files from below each.
        :param: dirs: str: a directory path (previous output)
        :return:
        """
        exclusions = []
        if type(dirs) is list:  # dirs is a list
            for directory in dirs:
                self.exclude_files_from(directory)
        elif "," in dirs:  # Assume that dirs is a comma separated list
            for directory in dirs.split(","):
                self.exclude_files_from(directory)
        else:
            for root, dirs, files in walk(dirs):
                for filename in files:
                    exclusions += [path.splitext(filename)[0]]

        self.exclude_regex_matches(list(set(exclusions)))

    def get_files(self):
        """
        Gather all files that match the input_regex that are below the input
        directory
        :return:
        """
        for root, _, files in walk(self.input_root):
            for filename in files:  # for all files
                if search(self.input_regex, filename):
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

    def module_cmd(self, args):
        """
        Load environment modules using environment module system
        :return:
        """
        try:
            # first argument is always 'load'
            args.extend(self.modules)  # add specified modules to arguments
            module(args)  # call module system, using arguments ['load', '...']
        except (OSError, ValueError) as err:
            if self.verbose:
                print("Could not load: {0:s}, {0:s}".format(self.modules,
                                                            err),
                      file=stderr)

    def make_directories(self):
        """
        Make the relative output directories that are necessary to preserve
        output directory structure at the specified output root. All directories
        below input_root will be created below output root
        :return:
        """
        directories = [x[0] for x in walk(self.input_root)]  # all dirs
        output_directories = [self.rebase_directory(x, self.input_root,
                                                    self.output_root)
                              for x in directories]  # rebase_directory each dir

        for directory in output_directories:
            if self.verbose:
                print("Attempting to make: {0:s}".format(directory),
                      file=stderr)
            if not self.dry_run:
                mkdir_p(directory)  # Attempt safe creation of each dir

    def run(self):
        """
        Run the Parallel Command from start to finish
        1) Load Environment Modules
        2) Gather input files
        3) Remove exclusions
        4) Make Directories
        5) Format Commands
        6) Dispatch Scripts to Cluster Scheduler
        7) Unload the modules
        """
        if self.verbose:
            print('Loading environment modules...', file=stderr)
        self.module_cmd(['load'])

        if self.verbose:
            print('Gathering input files...', file=stderr)
        self.get_files()

        if self.verbose:
            print('Removing exclusions...', file=stderr)

        if self.exclusions_paths:
            self.exclude_files_from(self.exclusions_paths)

        if self.exclusions:
            self.exclude_regex_matches(self.exclusions)

        if self.verbose:
            print("Making output directories...", file=stderr)
        self.make_directories()

        if self.verbose:
            print('Formatting commands...', file=stderr)
        self.format_commands()

        if self.verbose:
            print('Dispatching to cluster...', file=stderr)
        return (self.dispatch())  # Return the job IDs from the dispatched cmds

        if self.verbose:
            print("Unloading environment modules....", file=stderr)
        self.module_cmd(['unload'])


def mkdir_p(path):
    """
    Emulates UNIX `mkdir -p` functionality
    Attempts to make a directory, if it fails, error unless the failure was
    due to the directory already existing
    :param path: the path to make
    :return:
    """
    try:
        makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and isdir(path):
            print("{0:s} already exists".format(path),
                  file=stderr)
        else:
            raise exc


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
