#!/usr/bin/env python3
from itertools import product
from sys import argv
from Bash import bash
from io import BufferedReader
from io import TextIOWrapper
from os import remove
from os.path import isfile
from shutil import copyfileobj
from Decompress import decompress


def __is_ext(f, ext):
    return f.endswith(ext)


def __is_mapped(f):
    sb = [".sam", ".bam"]
    zip_ext = [""]  # Can add zip extensions if handling those filetypes
    sb_exts = ["".join(x) for x in product(sb, zip_ext)]
    return any([f.endswith(x) for x in sb_exts])


def __is_fq(f):
    fq = [".fq", ".fastq"]
    zip_ext = [".zip", ".bz2", ".gz", ""]
    fq_extensions = ["".join(x) for x in product(fq, zip_ext)]
    return any([f.endswith(x) for x in fq_extensions])


def __zipped(f):
    zip_ext = [".zip", ".bz2", ".gz"]
    return any([f.endswith(x) for x in zip_ext])


def combine_fq(file_list, output):
    if not output.endswith(".fq"):
        output += ".fq"

    # Don't overwrite output files
    if isfile(output):
        return

    # All files that are unzipped fq files, and all files that are zipped
    fq_files = [f for f in file_list if f.endswith(".fq")]
    fqz_files = [f for f in file_list if __zipped(f)]

    try:
        with open(output, 'w+') as o_h:
            for filename in fq_files:
                with open(filename, 'r+') as fh:
                    copyfileobj(fh, o_h, 1024 * 1024 * 10)
            for filename in fqz_files:  # java-like boilerplate for iteration
                with TextIOWrapper(BufferedReader(decompress(filename))) as gh:
                    copyfileobj(gh, o_h, 1024 * 1024 * 10)
    except (IOError, OSError) as err:
        remove(output)  # If something goes wrong, remove the output file
        raise (err)


def combine_alignments(file_list, output):
    assert(all([__is_mapped(x) for x in file_list]))
    command = ("java -Xmx{xmx} -Xms{xms} -Djava.io.tmpdir={tmp} -jar {picard} "
               "-T MergeSAMFiles {input_files} O={output_files}").format(
               xms="10G", xmx="12G", tmp="~/tmp",
               picard="~/.prog/picard-tools-2.5.0/picard.jar",
               input_files="".join(["I={} ".format(x) for x in file_list]),
               output_files=output
    )  # Use picard merge SAM to merge the files
    bash(command)


def combine_files(file_list, output="all_reads.fq"):
    """
    Add either fastq (.fq) or zipped fastq (.fq.xx) files to a file using gzip
    and cat in subprocess.
    """
    if all([__is_fq(x) for x in file_list]):
        combine_fq(file_list, output)
    elif all([__is_mapped(x) for x in file_list]):
        combine_alignments(file_list, output)


if __name__ == "__main__":
    if len(argv) == 3:
        combine_files(argv[1], argv[2])
    elif len(argv) == 2:
        combine_files(argv[1], "all_reads")
    else:
        raise (RuntimeError("Unexpected Argument Count"))
