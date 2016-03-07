#!/usr/bin/env python
from InterleavePaired import InterleavePaired
import sys


def main(input_root, output_root):
    i = InterleavePaired(input_root, output_root)
    i.job_prefix = "Map_"
    i.input_suffix = ".fq.gz"
    i.read_marker = "_1"
    i.mate_marker = "_2"
    i.modules = ['java']
    i.slurm_options['partition'] = 'bigmemm'
    i.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    i.slurm_options['mem'] = '20G'
    i.slurm_options['cpus'] = '2'
    i.verbose = True
    i.run()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
