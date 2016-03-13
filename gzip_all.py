#!/usr/bin/env python
from GzipAll import GzipAll
import sys


def main(input_, output_, choice):
    gz = GzipAll(input_, output_, choice)
    gz.slurm_options['mem'] = '10G'
    gz.slurm_options['cpus'] = '1'
    gz.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    gz.slurm_options['partition'] = 'bigmemm'
    gz.job_prefix = "GZ_"

    if choice.upper().strip() == "ZIP":
        gz.input_suffix = ".fq"
    else:
        gz.input_suffix = ".gz"

    gz.read_marker = "."
    gz.mate_marker = "."
    gz.verbose = False
    gz.dry_run = False
    gz.run()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
