#!/usr/bin/env python
from BBToolsMap import BBMapper
import sys


def main(input_root, output_root, reference, exclusions=""):
    m = BBMapper(input_root, output_root)
    m.job_prefix = "Map_"
    m.input_suffix = ".fq.gz"

    if "Piar" in input_root:
        m.read_marker = "_1"
        m.mate_marker = "_2"
    elif "Psme" in input_root:
        m.read_marker = "_R1"
        m.mate_marker = "_R2"

    m.modules = ['java']
    m.slurm_options['partition'] = 'bigmemm'
    m.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    m.slurm_options['mem'] = '300G'
    m.slurm_options['cpus'] = '26'
    m.reference = reference
    m.verbose = False
    m.dry_run = False
    m.exclusions_directory = exclusions
    m.run()


if __name__ == "__main__":
    if len(sys.argv) == 5:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    elif len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        main(sys.argv[1], sys.argv[2], "reference.fa")
