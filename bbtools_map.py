#!/usr/bin/env python
from BBToolsMap import BBMapper
import sys


def main(input_root, output_root, reference):
    m = BBMapper(input_root, output_root)
    m.job_prefix = "Map_"
    m.input_suffix = ".fq.gz"
    m.read_marker = "_1"
    m.mate_marker = "_2"
    m.modules = ['java']
    m.slurm_options['partition'] = 'bigmemm'
    m.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    m.slurm_options['mem'] = '80G'
    m.slurm_options['cpus'] = '10'
    m.reference = reference
    m.verbose = True
    m.run()


if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        main(sys.argv[1], sys.argv[2], "reference.fa")
