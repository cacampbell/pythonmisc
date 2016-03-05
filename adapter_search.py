#!/usr/bin/env python
from AdapterFinder import AdapterFinder
import sys


def main(input_root, output_root):
    af = AdapterFinder(input_root, output_root)
    af.job_prefix = 'FindAdapters_'
    af.input_suffix = '.fq.gz'
    af.modules = ['java']
    af.slurm_options['partition'] = 'bigmemh'
    af.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    af.slurm_options['mem'] = '80G'
    af.slurm_options['cpus'] = '10'
    af.run()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])