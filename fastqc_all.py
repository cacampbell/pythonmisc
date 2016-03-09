#!/usr/bin/env python
from FastqcAll import FastqcAll
import sys


def main(input_, output_):
    fqc = FastqcAll(input_, output_)
    fqc.slurm_options = {
        'mem': '10G',
        'tasks': '1',
        'cpus': '1',
        'job-name': '',
        'time': '0',
        'mail-user': 'cacampbell@ucdavis.edu',
        'mail-type': 'END,FAIL',
        'partition': 'bigmemm'
    }
    fqc.input_suffix = ".fq.gz"
    fqc.job_prefix = "Fastqc_"
    fqc.verbose = False
    fqc.dry_run = False
    fqc.run()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
