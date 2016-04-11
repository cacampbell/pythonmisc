#!/usr/bin/env python
from BBToolsMap import BBMapper
import sys


def main(input_root, output_root, exclusions=""):
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
    m.slurm_options['mem'] = '250G'
    m.slurm_options['cpus'] = '20'
    m.exclusions_directory = exclusions
    m.verbose = False
    m.dry_run = False
    m.run()


if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2],  sys.argv[3])
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print("Check Arguments")
