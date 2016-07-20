#!/usr/bin/env python3
from sys import stderr

from os.path import isfile
from os.path import join
from shutil import copyfileobj

from Bash import bash
from PairedEndCommand import PairedEndCommand
from ParallelCommand import mkdir_p


class TrinityAssemble(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(TrinityAssemble, self).__init__(*args, **kwargs)
        self.set_default("max_intron", "50000")
        self.set_default("genome_guided", False)
        self.set_default("contig_len", "250")
        self.modules = ['java']
        self.set_default("all_reads_name", "all_reads.bam")
        self.set_default("kmer_len", "25")

    def make_command(self, filename):
        pass

    def __merge_bam_files(self):
        bam_files = [f for f in self.files if f.endswith(".bam")]
        output_bam = join(self.input_root, self.all_reads_name)

        def __samtools_threads():
            return (str(min(self.get_threads(), 8)))


        if isfile(output_bam):
            print("Output BAM already exists, using it...", file=stderr)
            return(output_bam)

        try:
            (out, err) = bash("samtools -@ {t} -m {mem_p_t} "
                              "{o_bam} {i_bams}").format(t=__samtools_threads(),
                                                         mem_p_t="2G",
                                                         o_bam=output_bam,
                                                         i_bams=" ".join(
                                                             bam_files))

            if err:
                print(err, file=stderr)

        except Exception as error:
            print("Error while merging bam files: {}".format(error),
                  file=stderr)
            raise (error)

        return (output_bam)

    def __genome_guided_assembly(self):
        merged_bam = ""

        if not self.dry_run:
            merged_bam = self.__merge_bam_files()

        job_name = "{}".format(self.cluster_options["job_name"])

        command = ("Trinity --genome_guided_bam {mb} --genome_guided_max_intron"
                   " {mi} --max_memory {mem} --CPU {t} --output {o} "
                   "--KMER_SIZE={kmer_len} --min_contig_length {contiglen} "
                   "--full_cleanup").format(
            mb=merged_bam,
            mi=self.max_intron,
            mem=self.get_mem(fraction=0.95),
            t=self.get_threads(),
            o=self.output_root,
            contiglen=self.contig_len,
            kmer_len=self.kmer_len
        )

        self.commands[job_name] = command

        if self.verbose:
            print(command, file=stderr)

    def __merge_files(self):
        files = [f for f in self.files if "unmerged" not in f]

        if self.verbose:
            print("Merging single and paired end reads...", file=stderr)

        merged_files = []

        for merged in files:
            unmerged = self.replace_extension_with(".unmerged{}".format(
                self.extension), merged)
            combined = self.replace_extension_with(".combined{}".format(
                self.extension), merged)
            merged_files += [combined]

            try:
                if not isfile(combined):
                    with open(combined, 'wb') as combined:
                        with open(merged, 'rb') as merge:
                            copyfileobj(merge, combined, 1024 * 1024 * 10)
                        if isfile(unmerged):
                            with open(unmerged, 'rb') as unmerge:
                                copyfileobj(unmerge, combined, 1024 * 1024 * 10)
                else:
                    print("{} already exists, using it...".format(combined),
                          file=stderr)
            except (IOError, OSError) as err:
                print("Error while combining files: {}".format(err),
                      file=stderr)
                raise (err)

        return (merged_files)

    def __de_novo_assembly(self):
        merged_files = []

        if not self.dry_run:
            merged_files = self.__merge_files()

        job_name = "{}".format(self.cluster_options["job_name"])
        command = ("Trinity --seqType {type} --single {filelist} "
                   "--KMER_SIZE={kmer_len} --run_as_paired --max_memory {mem} "
                   "--CPU {t} --output {o} --min_contig_length "
                   "{contiglen}").format(
            type=self.extension.lstrip("."),
            filelist=",".join(merged_files),
            mem=self.get_mem(fraction=0.95),
            t=self.get_threads(),
            o=self.output_root,
            contiglen=self.contig_len,
            kmer_len=self.kmer_len
        )

        self.commands[job_name] = command

        if self.verbose:
            print(command, file=stderr)

    def format_commands(self):
        if self.genome_guided:
            self.__genome_guided_assembly()
        else:
            self.__de_novo_assembly()

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

        self.remove_regex_from_input(r".combine.{}".format(self.extension))
        self.remove_regex_from_input(r"{}".format(self.all_reads_name))

        if self.verbose:
            print('Formatting commands...', file=stderr)
        self.format_commands()

        if self.verbose:
            print('Dispatching to cluster...', file=stderr)
        jobs = self.dispatch()  # Return the job IDs from the dispatched cmds

        return (jobs)
