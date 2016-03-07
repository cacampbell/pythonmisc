#!/usr/bin/env python
from InterleavePaired import InterleavePaired
import sys


def main(input_root, output_root):
    i = InterleavePaired(input_root, output_root)
    i.job_prefix = "Interleave_"
    i.input_suffix = ".adap_trimmed.fastq.gz"
    i.read_marker = "_R1"
    i.mate_marker = "_R2"
    i.modules = ['java']
    i.slurm_options['partition'] = 'bigmemm'
    i.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    i.slurm_options['mem'] = '20G'
    i.slurm_options['cpus'] = '2'
    i.verbose = True
    i.dry_run = False
    i.run()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
    # input: /home/smontana/trimmed_cutadapt
    # output: /group/nealedata4/pear_diversity/interleaved_reads
