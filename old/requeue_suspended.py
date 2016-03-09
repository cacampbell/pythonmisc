#!/usr/bin/env python
import os
import subprocess
import re
import sys

p = subprocess.Popen(['squeue', '-u', 'cacampbe', '-l'], stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
(out, err) = p.communicate()
jobs = []

for line in out.split('\n'):
    if re.search('SUSPEND', line):
        jobs += [line.split()[0]]

for job in jobs:
    sys.stdout.write("{},".format(job))
