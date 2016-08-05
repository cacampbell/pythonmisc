#!/usr/bin/env python3
from Bash import mkdir_p
from os import listdir
from os import rename
from os.path import join
from os.path import basename
from sys import argv
from os.path import isfile
# Script to enter large raw directory for Illumina data, then split the files
# into sub-directories according to the first chunk of their names (the library
# name). This facilitates read grouping and individual analysis of samples.

exclusions = ["SDP7_H37FC_S7_L001_R1_001"]

def main(directory):
    filenames = [x for x in listdir(directory) if isfile(x)]

    for exclusion in exclusions:
        for filename in list(filenames):
            if exclusion in filename:
                filenames.remove(filename)

    libraries = list(set([basename(x).split("_")[0] for x in filenames]))
    [mkdir_p(join(directory, f)) for f in libraries]

    for lib in libraries:
        for filename in filenames:
            if lib in filename:
                initial = join(directory, filename)
                library = join(directory, lib)
                final = join(library, filename)

                try:
                    rename(initial, final)
                except Exception as err:
                    print(err)

if __name__ == "__main__":
    main(argv[1])
