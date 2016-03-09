#!/usr/bin/env python
from QualityControl import QualityControl
import sys


def main(input_root, output_root, adapters):
    qc = QualityControl(input_root, output_root)
    qc.job_prefix = "QC_"
    qc.input_suffix = ".fq.gz"
    qc.modules = ['java']
    qc.slurm_options['partition'] = 'bigmemm'
    qc.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    qc.slurm_options['mem'] = '80G'
    qc.slurm_options['cpus'] = '10'
    qc.reference = adapters
    qc.read_marker = "_1"
    qc.mate_marker = "_2"
    qc.verbose = False
    qc.dry_run = False
    qc.run()


if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        main(sys.argv[1], sys.argv[2], "adapters.fa")
