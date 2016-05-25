#!/usr/bin/env python
from __future__ import print_function

from PairedEndCommand import PairedEndCommand


class BBMapper(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        """
            Initialize this class using arguments passed to it

            Expected positional args:
                None

            Expcected kwargs:
                Unique to PairedEndCommand
                    :param: read_regex: str: regex for paired end files 1/2
                    :param: reference: str: the reference genome for mapping

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
        super(BBMapper, self).__init__(*args, **kwargs)
        self.set_default("reference", "reference.fa")

    def make_command(self, read):
        # Mate File
        mate = self.mate(read)

        # Mapped Sam file
        map_sam = self.replace_read_marker_with("_pe", read)
        map_sam = self.replace_extension(".sam", map_sam)
        map_sam = self.rebase_file(map_sam)

        # Unmapped Sam file
        unmap_sam = self.replace_read_marker_with("_pe", read)
        unmap_sam = self.replace_extension(".unmapped.sam", unmap_sam)
        unmap_sam = self.rebase_file(unmap_sam)

        # Coverage Statistics
        covstat = self.replace_read_marker_with("_covstats", read)
        covstat = self.replace_extension(".txt", covstat)
        covstat = self.rebase_file(covstat)

        # Coverage Hist
        covhist = self.replace_read_marker_with("_covhist", read)
        covhist = self.replace_extension(".txt", covhist)
        covhist = self.rebase_file(covhist)

        # Base Coverage
        basecov = self.replace_read_marker_with("_basecov", read)
        basecov = self.replace_extension(".txt", basecov)
        basecov = self.rebase_file(basecov)

        # Bin Coverage
        bincov = self.replace_read_marker_with("_bincov", read)
        bincov = self.replace_extension(".txt", bincov)
        bincov = self.rebase_file(bincov)

        # Full Command
        command = ("bbmap.sh in1={i1} in2={i2} outm={om} outu={ou} nodisk "
                   "covstats={covstat} covhist={covhist} threads={t} ref={r} "
                   "slow k=12 -Xmx{xmx} basecov={basecov} usejni=t"
                   " bincov={bincov}").format(i1=read,
                                              i2=mate,
                                              om=map_sam,
                                              ou=unmap_sam,
                                              covstat=covstat,
                                              covhist=covhist,
                                              basecov=basecov,
                                              bincov=bincov,
                                              xmx=self.get_mem(),
                                              t=self.get_threads(),
                                              r=self.reference)
        return (command)
