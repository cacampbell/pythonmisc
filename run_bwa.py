#!/usr/bin/env python
import sys
import os
import getopt
import fnmatch
import re
import itertools
import subprocess

# Global variables
CPUs = "36"
mem_per_cpu = "8000"
partition = "bigmemm"
temp_script = ".temp_script.sh"


class RunParameters:
    """
    Stores the Run Parameters for this script.
    """
    def validate(self):
        if self.root is None or self.root == "":
            print "Root not specified, using current directory"
            self.root = os.getcwd()
        else:
            print "Root Specified"

        if self.output is None or self.output == '':
            print "Output not specified, using ./BWA_Alignments"

            bwa_align = os.curdir + "BWA_Alignments"
            if not os.path.isdir(bwa_align):
                os.mkdir(bwa_align)

            self.output = bwa_align
        else:
            print "Output Specified"

        if self.genome is None or self.genome == "":
            raise RuntimeError("No reference genome specified")
        else:
            print "Genome Specified"

        self.root = os.path.abspath(self.root)
        self.genome = os.path.abspath(self.genome)
        self.root = os.path.abspath(self.root)

    def __init__(self, root, output, genome):
        self.root = root
        self.output = output
        self.genome = genome

    def __str__(self):
        return "root: " + self.root + " output: " + self.output + \
            " genome: " + self.genome


class BWA_Aligner:
    """
    Runs the Alignment for a given genome and set of transcriptome files
    """
    def run_commands(self):
        """
        Writes the bwa mem commands using information from self.commands
        """

        def make_command(args):
            """Make a single bwa mem command"""
            # args: genome_prefix file1 file2 > file1_v_2.outi
            bwa_command = "bwa mem -t" + CPUs + " " + args
            return bwa_command

        def write_bwa_script(bwa_command):
            """writes a temporary shell script to cwd"""
            with open(temp_script, "r+") as temp:
                temp.write("#!/usr/bin/env bash\n")
                temp.write(bwa_command + "\n")

        def run_bwa_script():
            """runs the temporary script"""
            slurm = "sbatch -N1 -n" + CPUs + " --mem-per-cpu=" + mem_per_cpu \
                + " --partition=" + partition + " "
            print "Attempting to run script: " + slurm + temp_script
            print "Contents of Script: "
            with open(temp_script, "r") as f:
                print f.read()
            subprocess.call(slurm + temp_script, shell=True)

        for args in self.command_args:
            try:
                bwa_command = make_command(args)
                subprocess.call("touch " + temp_script, shell=True)
                write_bwa_script(bwa_command)
                run_bwa_script()
            except Exception as e:
                print "Failed to make and / or execute script " + str(e)
            finally:
                subprocess.call("rm -rf " + temp_script, shell=True)

    def make_commands(self):
        """
        Builds the bwa mem commands from information collected in this class
        """
        def format_command_args(index, key, *pair):
            def outputfile():
                """
                Find the run number based on file name, making the hard
                assumption that the file path contains some variant of
                "_[0-9].fq.gz" within the file path provided. If no match
                to this format, insted uses the arbitrary index of the
                iteration through technical replicates to provide a unique
                output file name
                """
                pattern = re.compile("(?<=_)\d(?=\.fq.gz)")
                firstrun_m = pattern.search(pair[0])
                secondrun_m = pattern.search(pair[1])

                if firstrun_m and secondrun_m:
                    run1st = firstrun_m.group(0)
                    run2nd = secondrun_m.group(0)
                    return self.parameters.output + key \
                        + "_Run_" + run1st + "_v_Run_" + run2nd
                else:
                    return self.parameters.output + key + \
                        "_pair_" + str(index)

            return self.parameters.genome + " " + pair[0] + " " + pair[1] + \
                " > " + outputfile() + ".bwamem.out"

        for key in self.bwa_args:
            for index, pair in \
                    enumerate(itertools.combinations(self.bwa_args[key], r=2)):
                command = format_command_args(index, key, *pair)
                self.command_args.append(command)

    def get_filepair(self, prefix):
        """
        This will search the file list for the fasta files that are a family
        of technical replicates (runs).
        """
        tmp = []

        for f in self.filepaths:
            match = re.search(prefix, f)

            if match:
                tmp.append(f)

        assert(len(tmp) % 2 == 0)
        return tmp

    def get_pairs(self):
        """
        Get unique pairs of runs for the fasta files, based on filename
        """
        for f in self.files:
            prefix = re.match("^.*(?=\d\.fq.gz$)", f)

            if prefix:
                match = prefix.group(0)

                if match not in self.bwa_args:
                    self.bwa_args[match] = self.get_filepair(match)

    def get_files(self):
        """
        Get the fasta files by reading through the directory
        structure
        """
        for root, dirnames, filenames in os.walk(self.parameters.root):
            for filename in fnmatch.filter(filenames, "*.fq.gz"):
                self.files.append(filename)
                self.filepaths.append(os.path.join(root, filename))

    def run_BWA(self):
        """
        Run BWA mem with each unique pair against the reference genome
        """
        self.get_files()
        self.get_pairs()
        self.make_commands()
        self.run_commands()

    def __init__(self, runparam):
        """
        Initialized with a RunParameters object that contains the path
        information for the alignment
        """
        self.parameters = runparam
        self.files = []
        self.filepaths = []
        self.bwa_args = {}
        self.command_args = []


def Run(runparam):
    """
    Perform the analysis described in main. Gather the transcipt files and
    use the BWA mem program to align them to the reference genome
    """
    bwa = BWA_Aligner(runparam)
    bwa.run_BWA()


def getArgs(optlist):
    """
    Gets the Expected arguments from the given optionlist, which are the
    reference genome, output directory, and root directory
    """
    root = ""
    output = ""
    genome = ""

    for option, argument in optlist:
        if option in ("-h", "--help", "help"):
            print "Call with no arguments to see usage"
            sys.exit()
        elif option in ("-o", "--output"):
            output = argument
        elif option in ("-r", "--root"):
            root = argument
        elif option in ("-g", "--genome", "--reference"):
            genome = argument

    return RunParameters(root, output, genome)


def main(argv):
    """
    Gather the transcriptome files, group them into unique
    collections of two files each, then run the bwa mem program using
    the reference genome.

    Expected Keywords:
        -r [root directory] -- the root directory that contains all
                               transcriptome files below it

        -o [output directory] -- the output directory for the
                                 alignments

        -g [reference genome prefix] -- the reference genome database
                                        prefix
    """

    try:
        optlist, args = getopt.getopt(argv, "r:o:g:")
    except getopt.GetoptError as err:
        print str(err)
        print __doc__
        sys.exit()

    if len(optlist) == 0:
        print "No Arguments specified"
        sys.exit()

    params = getArgs(optlist)

    try:
        params.validate()
    except RuntimeError as no_genome:
        print no_genome
        sys.exit(2)

    try:
        os.chdir(params.output)
    except:
        print "Could not change directory to: ", params.output
        sys.exit(2)

    Run(params)

if __name__ == "__main__":
    main(sys.argv[1:])
