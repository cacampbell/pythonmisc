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
from os.path import isdir, basename
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

    def __init__(self, *args, **kwargs):
        """
        __init__ Initialize this class with arguments passed to it.

        PairedEndCommand proceeds as follows:
        1) Load environment modules
        2) Gather input files
        3) Remove exclusions from input files
        4) Make output directories
        5) Format commands based on make_command method
        6) Send commands to wrapper for cluster scheduler using options
        7) Unload environment modules

        Expected positional args:
            None

        Expcected kwargs:
            :param: input_root: str: the input root
            :param: output_root: str: the output root
            :param: input_regex: str: regex specifying all input files
            :param: extension: str: regex for extension on files
            :param: exclusions: str: regex or python list that specifies which
                    files are to be excluded from the input files of this run
            :param: exlcusions_paths: str: directory path or comma-separated
                    list of directory names that each contain files with a
                    basename that you wish for this class to skip during run()
                    that should be excluded from the given run
            :param: dry_run: bool: Toggles whether or not commands are actually
                    run
            :param: verbose: bool: Toggles print statements throughout class
            :param: cluster_options: dict<str>: dictionary of cluster options
                memory - The memory to be allocated to this job
                nodes - The nodes to be allocated
                cpus - The cpus **per node** to request
                partition -  The queue name or partition name for the submitted
                job
                job_name - common prefix for all jobs created by this instance
                depends_on - The dependencies (as comma separated list of job
                numbers)
                email_address -  The email address to use for notifications
                email_options - Email options: BEGIN|END|FAIL|ABORT|ALL
                time - time to request from the scheduler
                bash -  The bash shebang line to use in the script


        Any other keyword arguments will be added as an attribute with that name
        to the instance of this class. So, if additional parameters are needed
        for formatting commands or any other overriden methods, then they
        can be specified as a keyword agument to init for convenience.

        For example, bbtools_map.py uses a --stats flag to determine whether or
        not the user wants to output mapping statistics alongside the mapping.

        Many commands use a reference genome or some additional data files, you
        could specify these by adding --reference="reference.fa" to the input
        and then invoking "self.reference" in the make_command method.
        """
        self.input_root = None
        self.output_root = None
        self.input_regex = None
        self.modules = None
        self.extension = "\.fq\.gz"
        self.exclusions = None
        self.exclusions_paths = None
        self.dry_run = False
        self.verbose = False
        self.cluster_options = dict(memory="2G",
                                    nodes="1",
                                    cpus="1",
                                    partition="normal",
                                    job_name="ParallelCommand_",
                                    depends_on=None,
                                    email_address=None,
                                    email_options=None,
                                    time=None,
                                    bash="#!/usr/bin/env bash"
                                    )  # End dict
        # Set defualts for some required keywords:
        self.set_default("input_root", getcwd())
        self.set_default("output_root", getcwd())
        self.set_default("input_regex", ".*")

        for key, value in kwargs.items():
            try:
                if key == "cluster_options":  # Set cluster options
                    self.cluster_options = {**self.cluster_options, **value}
                else:
                    setattr(self, key, value)  # Try to set each new attribute
            except Exception as err:
                print("Could not set attribute: {}".format(key), file=stderr)
                raise (err)

        self.files = []
        self.commands = {}

    def set_default(self, attribute, default_value):
        """
        Check that an attribute exists and that it has a non-None value, then
        set that attribute to a value if it does not, for this instance
        :param attribute: str: the name of the attribute to check and set
        :param default_value: <obj>: the default value for the attribute
        :return:
        """
        try:
            assert (getattr(self, attribute) is not None)
        except (AssertionError, AttributeError) as err:
            setattr(self, attribute, default_value)

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
        return "{}{}".format(memory, mem_unit)  # [\d][c], memory + units

    def dispatch(self):
        """
        For each command in self.commands, submit that command to the cluster
        scheduler using the desired options from self.cluster_options
        """
        job_numbers = []

        for job_name, command in self.commands.items():
            # If the job is not already running and actually submitting the job
            if job_name not in existing_jobs() and not self.dry_run:
                # Replace the job name for the cluster options copy (per job)
                opts = dict(self.cluster_options)
                opts["job_name"] = job_name
                opts["output"] = "{}_output".format(job_name)
                opts["error"] = "{}_error".format(job_name)

                # Capture the job number for the submitted job
                job_number = submit_job(command, **opts)
                job_numbers.append(job_number)

                if self.verbose:
                    print("Submitted job: {}".format(job_number), file=stderr)
            else:
                print("Job {} already running or dry_run set to True".format(
                    job_name
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
        for filename in self.files:  # for each file
            command = ""
            job_name = "{}{}".format(self.cluster_options["job_name"],
                                     basename(filename))

            try:
                command = self.make_command(filename)  # derived class command
            except Exception as ex:
                if self.verbose:
                    print("Command formatting failed: {}".format(ex),
                          file=stderr)

            assert (type(command) is str)  # at least, it has to be a str
            self.commands[job_name] = command

            if self.verbose:
                print(command, file=stderr)

    def remove_regex_from_input(self, regex):
        if type(regex) is list:
            for r in regex:
                self.remove_regex_from_input(r)

        for filename in list(self.files):
            if search(regex, filename):
                self.files.remove(filename)

                if self.verbose:
                    print("Removed: {}".format(filename), file=stderr)

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
                print("Could not load: {}, {}".format(self.modules,
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
                print("Attempting to make: {}".format(directory),
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
        :return: list<str>: a list of job IDs returned by cluster scheduler
        """
        if self.verbose:
            print('Loading environment modules...', file=stderr)
            if self.modules is not None:
                self.module_cmd(['load'])

        if self.verbose:
            print('Gathering input files...', file=stderr)
        self.get_files()

        if self.verbose:
            print('Removing exclusions...', file=stderr)

        if self.exclusions_paths:
            self.remove_files_below(self.exclusions_paths)

        self.remove_files_below(self.output_root)

        if self.exclusions:
            self.remove_regex_from_input(self.exclusions)

        if self.verbose:
            print("Making output directories...", file=stderr)
        self.make_directories()

        if self.verbose:
            print('Formatting commands...', file=stderr)
        self.format_commands()

        if self.verbose:
            print('Dispatching to cluster...', file=stderr)
        jobs = self.dispatch()  # Return the job IDs from the dispatched cmds

        if self.verbose:
            print("Unloading environment modules....", file=stderr)
            if self.modules is not None:
                self.module_cmd(['unload'])

        return (jobs)


def mkdir_p(p):
    """
    Emulates UNIX `mkdir -p` functionality
    Attempts to make a directory, if it fails, error unless the failure was
    due to the directory already existing
    :param: p: str: the path to make
    :return:
    :raises: OSError: If not "directory already exists"
    """
    try:
        makedirs(p)
    except OSError as err:
        if err.errno == errno.EEXIST and isdir(p):
            print("{} already exists".format(p),
                  file=stderr)
        else:
            raise (err)


class TestParallelCommand(unittest.TestCase):
    # Some day, I will write these unit tests
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
