#!/usr/bin/env python
from ZipAll import ZipAll
import sys


def main(input_, output_, choice):
    z = ZipAll(input_, output_, choice)
    z.slurm_options['mem'] = '10G'
    z.slurm_options['cpus'] = '1'
    z.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    z.slurm_options['partition'] = 'bigmemm'
    z.job_prefix = "z_"

    if choice.upper().strip() == "ZIP":
        z.input_suffix = ".*"
    else:
        z.input_suffix = ".zip"

    z.read_marker = "."
    z.mate_marker = "."
    z.verbose = False
    z.dry_run = False
    z.run()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
