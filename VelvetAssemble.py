#!/usr/bin/env python3
from sys import stderr

from os import walk
from os.path import isfile
from shutil import copyfileobj

from Bash import bash
from Bash import mkdir_p
from PairedEndCommand import PairedEndCommand


class VelvetAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(VelvetAssemble, self).__init__(*args, **kwargs)
        self.set_default("startk", "21")
        self.set_default("endk", "37")
        self.set_default("contig_len", "250")
        self.set_default("reference_guided", False)
        self.set_default("reference", "reference.fa")
        self.set_default("all_merged", "{}/all_merged".format(self.input_root))

    def make_command(self, filename):
        pass

    def __merge_fastq(self, files, combined_name):
        try:
            if not isfile(combined_name):
                with open(combined_name, 'wb') as combined:
                    for filename in files:
                        with open(filename, 'rb') as merge:
                            copyfileobj(merge, combined, 1024 * 1024 * 10)
            else:
                print("{} already exists, using it...".format(combined_name),
                      file=stderr)
        except (IOError, OSError) as err:
            print("Error while combining files: {}".format(err), file=stderr)
            raise (err)

    def __merge_bam(self, files, combined_name):
        try:
            if not isfile(combined_name):
                bash("samtools merge {} {}".format(combined_name,
                                                   " ".join(files)))
            else:
                print("{} already exists, using it...".format(combined_name),
                      file=stderr)
        except (IOError, OSError) as err:
            print("Error while combining files: {}".format(err), file=stderr)
            raise (err)

    def __merge_files(self, files, combined_name):
        if all(x.endswith(".fq") for x in files):
            self.__merge_fastq(files, combined_name)
        elif all(x.endswith(".bam") for x in files):
            self.__merge_bam(files, combined_name)

    def __str_lib(self, merged_libraries, guided=False):
        lib_str = ""
        single_end_files = []

        # make files into velvet formatted library string
        # order of library specification does not matter
        if not guided:
            # Reference Guided, assume bam format
            for (i, key) in enumerate(merged_libraries.keys()):
                unmerged_f = merged_libraries[key][0]
                single_end_files += [merged_libraries[key][1]]
                lib_str += '-shortPair{} -fastq {}'.format(i + 1, unmerged_f)

            self.__merge_files(single_end_files, self.all_merged)
            lib_str += ' -short -fastq {}'.format(self.all_merged)
        else:
            # Not Reference Guided, assume fastq format
            for (i, key) in enumerate(merged_libraries.keys()):
                unmerged_f = merged_libraries[key][0]
                single_end_files += [merged_libraries[key][1]]
                lib_str += '-shortPair{} -bam {}'.format(i + 1, unmerged_f)

            self.__merge_files(single_end_files, self.all_merged)
            lib_str += ' -short -bam {}'.format(self.all_merged)
        return (lib_str)

    def __format_libraries(self, guided=False):
        # self.files --> {"library name":[[unmerged_files], [merged_files]]}
        # TODO: This can definitely be refactored...
        libraries = {}
        merged_libraries = {}
        libnames = [x for x, dirs, files in walk(self.input_root) if not dirs]
        unmerged = [x for x in self.files if "unmerged" in x]
        merged = [x for x in self.files if not "unmerged" in x]

        for name in libnames:
            libraries[name] = [
                [x for x in unmerged if name in x],
                [x for x in merged if name in x]
            ]  # for each library, add unmerged files and merged files

        # {"name":[[unmerged], [merged]} --> {"name":[unmerged_f, merged_f]}
        # From a list of files per library to merged files for each library
        for name in libraries.keys():
            prefix = name.replace("/", "_")
            unmerged_f = "{}_unmerged.fq".format(prefix)
            merged_f = "{}_merged.fq".format(prefix)
            self.__merge_files(libraries[name][0], unmerged_f)
            self.__merge_files(libraries[name][1], merged_f)
            merged_libraries[name] = [unmerged_f, merged_f]

        return (self.__str_lib(merged_libraries, guided=guided))

    def __command(self):
        command = ("velvetoptimiser.pl --hashs={startk} --hashe={endk} -t "
                   "{threads} --optFuncKmer 'n50*Lcon/tbp+log(Lbp)' "
                   "--velvetgoptions='-min_contig_lgth {contig_len}' --d "
                   "{out} -f '{libraries}").format(
            startk=self.startk,
            endk=self.endk,
            unmerged=self.unmerged,
            merged=self.merged,
            threads=self.get_threads(),
            contig_len=self.contig_len,
            out=self.output_root,
            libraries=self.__format_libraries(guided=self.reference_guided)
        )  # Command

        if self.reference_guided:
            command += " -reference={}'".format(self.reference)
        else:
            command += "'"

        return command

    def format_commands(self):
        command = self.__command()
        job_name = "{}".format(self.cluster_options["job_name"])
        self.commands[job_name] = command

        if self.verbose:
            print(command, file=stderr)

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
