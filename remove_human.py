#!/usr/bin/env python
from RemoveHuman import RemoveHuman
import sys


def main(input_root, output_root, reference):
    rh = RemoveHuman(input_root, output_root)
    rh.job_prefix = "RH_"
    rh.input_suffix = ".fq.gz"
    rh.modules = ['java']
    rh.slurm_options['partition'] = 'bigmemh'
    rh.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    rh.slurm_options['mem'] = '80G'
    rh.slurm_options['cpus'] = '10'
    rh.read_marker = "_1"
    rh.mate_marker = "_2"
    rh.reference = reference
    rh.dry_run = False
    rh.verbose = False
    rh.run()


if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        main(sys.argv[1], sys.argv[2], ("/group/nealedata4/Psme_reseq/qc/Hosa_"
                                        "masked/hg19_main_mask_ribo_animal_"
                                        "allplant_allfungus.fa.gz"))
