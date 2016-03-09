#!/usr/bin/env python
from RecalibrateBases import RecalibrateBases
import sys


def main(input_, output_):
    rb = RecalibrateBases(input_, output_)
    rb.slurm_options['partition'] = 'bigmemm'
    rb.slurm_options['mem'] = '200G'
    rb.slurm_options['cpus'] = '24'
    rb.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    rb.modules = ['java']
    rb.input_suffix = '.fq.gz'
    rb.verbose = False
    rb.dry_run = False
    rb.run()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])