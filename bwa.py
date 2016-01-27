from getopt import getopt, GetoptError
from subprocess import check_output
import os
import sys
import fnmatch

#  Script to write and dispatch bwa alignments to the slurm resource manager
#  Input: See help for this script; input directory, reference genome
#  Output: Alignments, Slurm logs
#  This script is used for the specific case of paired end reads in a read-mate
#  relationship designated by their names (R1 and R2) respectively
#  This script descends into a directory and finds all possible read files, then
#  attempts to match them to a mate and, if unsucessful, runs bws assuming a
#  single end read
#  It is also assumed by this script that the reads have previously been cleaned
#  of adpater sequences and made ready for analysis


class Read:
    def __init__(self, read, mate=""):
        self.run_one = read
        self.run_two = mate


class BWA_Alignment:
    def _get_bwa(self):
        return check_output("module load bwa", shell=True).strip()

    def _get_flags(kwargs):
        returnval = ""

        for key, value in kwargs.viewitems():
            returnval = returnval + key + " " + value + " "

        return returnval

    def __init__(self, **kwargs):
        self.flags = self._get_flags(kwargs)
        self.executable = self._get_bwa()
        self.command = self.executable + " mem " + self.flags


class SystemParameters:
    def __init__(self, cpus="30", mpc="8000",  partition="bigmemm"):
        self.cores = cpus
        self.mem_per_core = mpc
        self.cluster_partition = partition


class RunParameters:
    def __init__(self, genome, indir, outdir):
        self.reference_genome = genome
        self.sequence_root = indir
        self.output_root = outdir


class Slurm_Dispatch:
    def run_commands():
        pass

    def __init__(self, commands):
        self.commands = commands


def get_files(sequence_root, verbose):
    filepaths = []
    if verbose:
        print "Gathering files..."

    for root, dirnames, filenames in os.walk(sequence_root):
        for filename in fnmatch.filter(filenames, "*.fq.gz"):
            if verbose:
                print "Found: " + os.path.join(root, filename)
            filepaths.append(os.path.join(root, filename))

    if verbose:
        print "Finished gathering files"
    return filepaths


def get_pairs(files, verbose):
    pass


def make_commands(command, pairs, verbose):
    pass


def Run(system, run, verbose, test):
    bwa = BWA_Alignment()
    files = get_files(run.sequence_root, verbose)
    pairs = get_pairs(files, verbose)
    commands = make_commands(bwa.command, pairs, verbose)
    slurm = Slurm_Dispatch(run, commands, verbose, test)
    slurm.run_commands()


def main(command_line_arguments):

    def usage():
        pass

    try:
        opts, args = getopt(command_line_arguments, "htvi:o:r:")
    except GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    reference_genome = None
    sequence_root = None
    output_root = None
    verbose = False
    test = False

    for option, argument in opts:
        if option in ("-v", "--verbose"):
            verbose = True
        elif option in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif option in ("-t", "--test"):
            test = True
        elif option in ("-i", "--input="):
            sequence_root = argument
        elif option in ("-o", "--output="):
            output_root = argument
        elif option in ("-r", "--ref"):
            reference_genome = argument

    system = SystemParameters()
    run = RunParameters(reference_genome, sequence_root, output_root)
    Run(system, run, verbose, test)


if __name__ == "__main__":
    main(sys.argv[1:])
