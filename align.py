#!/usr/bin/env python
from BBMapper import BBMapper
import sys


def main(input_root, output_root, reference):
    m = BBMapper(input_root, output_root)
    m.job_prefix = "Map_"
    m.job_prefix = ".fq.gz"
    m.modules = ['java']
    m.slurm_options['partition'] = 'bigmemm'
    m.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    m.slurm_options['mem'] = '80G'
    m.slurm_options['cpus'] = '10'
    m.reference = reference
    m.run()


if __name__ == "__main__":
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        main(sys.argv[1], sys.argv[2], "reference.fa")