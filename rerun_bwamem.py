#!/usr/bin/env python
import re
import os
import subprocess
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



find = 'find /share/nealedata2/bwa_Comice -size 0'
out = subprocess.Popen(find, shell=True, stdin=subprocess.PIPE,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
(stdout, stderr) = out.communicate()
filelist = stdout.decode().split()
for filename in filelist:
    if re.search("002", filename):
        abs_filename = os.path.join(directory, os.path.basename(filename))
        in_prefix = re.sub(r"_002_pe.sam$", "", abs_filename)
        out = os.path.join(output, filename)
        cmd = "bwa mem -t 30 %s %s_R1_002.fastq.gz %s_R2_002.fastq.gz > %s" \
            % (genome, in_prefix, in_prefix, out)
        dispatchscript(cmd)
    elif re.search("001", filename):
        abs_filename = os.path.join(directory, os.path.basename(filename))
        in_prefix = re.sub(r"_001_pe.sam$", "", abs_filename)
        out = os.path.join(output, filename)
        cmd = "bwa mem -t 30 %s %s_R1_001.fastq.gz %s_R2_001.fastq.gz > %s" \
            % (genome, in_prefix, in_prefix, out)
        dispatchscript(cmd)
