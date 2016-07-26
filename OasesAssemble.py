#!/usr/bin/env python3
from sys import stderr

from Bash import mkdir_p
from VelvetAssemble import VelvetAssemble


class OasesAssemble(VelvetAssemble):
    def __init__(self, *args, **kwargs):
        super(OasesAssemble, self).__init__(*args, **kwargs)
        self.set_default("startk", "21")
        self.set_default("endk", "37")
        self.set_default("contig_len", "250")
        self.set_default("reference_guided", False)
        self.set_default("reference", "reference.fa")
        self.set_default("all_merged", "{}/all_merged".format(self.input_root))

    def make_command(self, filename):
        pass

    def format_commands(self):
        job_name = "{}".format(self.cluster_options["job_name"])
        command = ("export OMP_NUM_THREADS={omp} oases_pipeline.py -m {startk} "
                   "-M {endk} -p '-min_contig_lgth {contig_len}' "
                   "-o {out} --data '{libraries}").format(
            startk=self.startk,
            endk=self.endk,
            threads=self.get_threads(),
            contig_len=self.contig_len,
            out=self.output_root,
            libraries=self.format_libraries(guided=self.reference_guided),
            omp=self.get_threads(0.95)
        )  # Command

        if self.reference_guided:
            command += " -reference={}'".format(self.reference)
        else:
            command += "'"

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
