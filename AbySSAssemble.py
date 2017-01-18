#!/usr/bin/env python3
from PairedEndCommand import PairedEndCommand
from sys import stderr
from Bash import mkdir_p
from re import search
from os.path import splitext
from os.path import basename
from os.path import join
from os import walk


class AbySSAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(AbySSAssemble, self).__init__(*args, **kwargs)
        self.set_default('kmer_len', None)
        self.set_default('max_bubble_branches', None)
        self.set_default('max_bubble_len', None)
        self.set_default('min_unitig_cov', None)
        self.set_default('min_unitig_len', None)
        self.set_default('min_contig_len', None)

    def format_commands(self):
        name = "{}".format(self.cluster_options['job_name'])
        verbose = None
        if self.verbose:
            verbose = '-vv'
        args = {
            'k': self.kmer_len,
            'v': verbose,
            'a': self.max_bubble_branches,
            'b': self.max_bubble_len,
            'c': self.min_unitig_cov,
            'j': self.get_threads(),
            's': self.min_unitig_len,
            'S': self.min_contig_len,
            'name': name
        }

        libs = {}
        # Alright, this is terrible, but it had to be done to fix
        # an immediate issue. Rather than constructing the libraries
        # not like an idiot, here, I use the implicit 'mate_regex'
        # to identify files in the gathered list that must be removed
        mate_regex = self.read_regex.replace('1', '2')  # *barfs*
        for filename in list(self.files):
            filename_strip = splitext(basename(filename))[0]
            if search(mate_regex, filename_strip):
                self.files.remove(filename)

        for filename in self.files:
            filename_strip = splitext(basename(filename))[0]
            if search(self.read_regex, filename_strip):
                libname = self.replace_read_marker_with('', filename_strip)
                libname = "pe_{}".format(libname)
                libs[libname] = [filename, self.mate(filename)]
            else:
                se_key = 'se'
                if se_key in libs.keys():
                    libs[se_key] += [filename]
                else:
                    libs[se_key] = [filename]

        command = 'abyss-pe -C {}'.format(self.output_root)

        # AbySS implements a dry_run mode, so this flag is included in
        # the command. This will never be run, except in the case that
        # the dry_run command is copy and pasted to the terminal
        if self.dry_run:
            command += ' --dry-run'

        # For each flag, append to the command
        for flag, arg in args.items():
            if arg:
                command += " {}={}".format(flag, arg)

        # Abyss requires that library names be listed first in lib param
        command += ' lib="{}"'.format(" ".join(libs.keys()))

        # Then, for each library name, the filenames are listed
        for lib, files in libs.items():
            command += ' {}="{}"'.format(lib, " ".join(files))

        # Now append on the assembly contiguity assessment script
        command += ' && abyss-fac -C {} *.fa'.format(self.output_root)

        self.commands[name] = command

        if self.verbose:
            print(command, file=stderr)

    def make_command(self, read):
        pass

    def get_files(self):
        """
        Gather all files that match the input_regex that are below the input
        directory
        :return:
        """
        for root, _, files in walk(self.input_root, followlinks=True):
            for filename in files:  # for all files
                # Match input_regex only -- do not match read regex for input
                # files in this case, since we need to find single end
                if search(self.input_regex, filename):
                    if self.extension is not None:
                        if search(self.extension, filename):
                            abs_path = join(root, filename)
                            self.files += [abs_path]

                            if self.verbose:
                                print(abs_path, file=stderr)
                    else:
                        abs_path = join(root, filename)
                        self.files += [abs_path]

                        if self.verbose:
                            print(abs_path, file=stderr)


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

        if self.verbose:
            print("Making output directories...", file=stderr)
        mkdir_p(self.output_root)

        if self.exclusions_paths:
            self.exclude_files_below(self.exclusions_paths)

        self.exclude_files_below(self.output_root)

        if self.exclusions:
            self.remove_regex_from_input(self.exclusions)

        if self.verbose:
            print('Formatting commands...', file=stderr)
        self.format_commands()

        if self.verbose:
            print('Dispatching to cluster...', file=stderr)
        jobs = self.dispatch()  # Return the job IDs from the dispatched cmds

        return (jobs)
