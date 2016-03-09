#!/usr/bin/env python
import re
import os
from subprocess import call
from clusterlib.scheduler import submit

directory = "/home/smontana/alignements"
genome = os.path.join(directory, "indexed_DH_Comice")
output = "/home/cacampbe/pear_bwa/sam_files"
slurm_logs = "/home/cacampbe/pear_bwa/slurm_out"


def dispatchscript(cmd):
    script = submit(cmd, memory="240000", job_name="bwa_mem", backend="slurm",
                    time="0", shell_script="#!/usr/bin/env bash",
                    log_directory=slurm_logs)
    script = script + " --partition=bigmemm"
    script = script + " --cpus-per-task=30"
    print "\nCalling:\n%s\n" % script
    call("%s" % script, shell=True)


for root, directories, filenames in os.walk(os.path.abspath(directory)):
    for filename in filenames:
        if filename.endswith(".gz"):
            abs_file = os.path.join(root, filename)
            in_prefix = re.sub(r"_R[12]_00[12]\.fastq\.gz", "", abs_file)
            rel_prefix = re.sub(r"_R[12]_00[12]\.fastq\.gz", "", filename)
            out_prefix = os.path.join(output, rel_prefix)
            cmd1 = "bwa mem -t 30 %s %s_R1_001.fastq.gz %s_R2_001.fastq.gz > \
                %s_001_pe.sam" % (genome, in_prefix, in_prefix, out_prefix)
            cmd2 = "bwa mem -t 30 %s %s_R1_002.fastq.gz %s_R2_002.fastq.gz > \
                %s_002_pe.sam" % (genome, in_prefix, in_prefix, out_prefix)
            dispatchscript(cmd1)
            dispatchscript(cmd2)
