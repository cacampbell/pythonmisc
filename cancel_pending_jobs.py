#!/usr/bin/env python
from slurm_commands import squeue, scancel
import pwd
import os

job_list = []
who = pwd.getpwuid(os.getuid()).pw_name

for line in squeue("-u {} -l -h -t {}".format(who, "PD"))[0]:
    job_list.append(line.split()[0])

scancel(" ".join(job_list))
