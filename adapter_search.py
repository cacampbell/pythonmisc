#!/usr/bin/env python
from AdapterSearch import AdapterFinder
import sys


def main(input_root, output_root):
    af = AdapterFinder(input_root, output_root)
    af.job_prefix = 'FA_'
    af.input_suffix = '.fq.gz'
    af.read_marker = "_R1"
    af.mate_marker = "_R2"
    af.modules = ['java']
    af.slurm_options['partition'] = 'bigmemh'
    af.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    af.slurm_options['mem'] = '80G'
    af.slurm_options['cpus'] = '10'
    af.verbose = False
    af.dry_run = False
    af.run()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
