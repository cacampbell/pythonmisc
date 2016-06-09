from PairedEndCommand import PairedEndCommand


class AdapterFinder(PairedEndCommand):
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
            Unique to PairedEndCommand
                :param: read_regex: str: regex for first of the paired end files

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
        super(AdapterFinder, self).__init__(*args, **kwargs)
        # self.read_regex = "_R1(?=\.fq)"

    def make_command(self, filename):
        mate = self.mate(filename)
        adapter = self.replace_read_marker_with("_Adapter", filename)
        adapter = self.replace_extension(".fa", adapter)
        adapter = self.rebase_file(adapter)
        command = ("bbmerge.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} "
                   "outa={o}").format(xmx=self.get_mem(fraction=0.95),
                                      t=self.get_threads(),
                                      i1=filename,
                                      i2=mate,
                                      o=adapter)
        return (command)
