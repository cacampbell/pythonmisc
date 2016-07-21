#!/usr/bin/env python3
from sys import stderr

from os.path import isfile
from shutil import copyfileobj

from Bash import mkdir_p
from PairedEndCommand import PairedEndCommand


class VelvetAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(VelvetAssemble, self).__init__(*args, **kwargs)
        self.set_default("startk", "21")
        self.set_default("endk", "37")
        self.set_default("contig_len", "250")

    def make_command(self, filename):
        pass

    def __merge_files(self, files, merged_name):
        try:
            if not isfile(merged_name):
                with open(merged_name, 'wb') as combined:
                    for filename in files:
                        with open(filename, 'rb') as merge:
                            copyfileobj(merge, combined, 1024 * 1024 * 10)
            else:
                print("{} already exists, using it...".format(merged_name),
                      file=stderr)
        except (IOError, OSError) as err:
            print("Error while combining files: {}".format(err),
                  file=stderr)
            raise (err)

    def format_commands(self):
        merged_reads = [x for x in self.files if not "unmerged" in x]
        unmerged_reads = [x for x in self.files if "unmerged" in x]
        job_name = "{}".format(self.cluster_options["job_name"])

        self.set_default("unmerged", "{}/velvet_combined_pairs.fq".format(
            self.input_root))
        self.set_default("merged", "{}/velvet_combined_singles.fq".format(
            self.input_root))

        if not self.dry_run:
            if self.verbose:
                print("Merging input files...", file=stderr)

            self.__merge_files(merged_reads, self.merged)
            self.__merge_files(unmerged_reads, self.unmerged)

        command = ("velvetoptimiser.pl --hashs={startk} "
                   "--hashe={endk} -f '-shortPaired "
                   "-fastq {unmerged} -long -fastq "
                   "{merged}' -t {threads} --optFuncKmer "
                   "'n50*Lcon/tbp+log(Lbp)' --velvetgoptions="
                   "'-min_contig_lgth {contig_len}' --d {out}").format(
            startk=self.startk,
            endk=self.endk,
            unmerged=self.unmerged,
            merged=self.merged,
            threads=self.get_threads(),
            contig_len=self.contig_len,
            out=self.output_root
        )  # Command

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
