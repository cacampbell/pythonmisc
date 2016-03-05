#!/usr/bin/env python
from RemoveHuman import RemoveHuman
import sys


def main(input_root, output_root, reference):
    rh = RemoveHuman(input_root, output_root)
    rh.job_prefix = "RH_"
    rh.job_prefix = ".fq.gz"
    rh.modules = ['java']
    rh.slurm_options['partition'] = 'bigmemm'
    rh.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    rh.slurm_options['mem'] = '80G'
    rh.slurm_options['cpus'] = '10'
    rh.reference = reference
    rh.run()


if __name__ == "__main__":
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        main(sys.argv[1], sys.argv[2], ("/group/nealedata4/Psme_reseq/qc/Hosa_"
                                        "masked/hg19_main_mask_ribo_animal_"
                                        "allplant_allfungus.fa.gz"))
