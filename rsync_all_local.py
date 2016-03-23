#!/usr/bin/env python
from RsyncAllLocal import RsyncAllLocal
import sys


def main(input_, output_):
    r = RsyncAllLocal(input_, output_)
    r.input_suffix = ".$"
    r.slurm_options['partition'] = 'serial'
    r.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    r.slurm_options['mem'] = '2G'
    r.slurm_options['cpus'] = '1'
    r.dry_run = False
    r.verbose = True
    r.run()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
