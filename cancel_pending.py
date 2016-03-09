#!/usr/bin/env python
import os
import subprocess
import re
import sys

p = subprocess.Popen(['squeue', '-u', 'cacampbe', '-l', '-p', 'bigmemm'],
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
(out, err) = p.communicate()
jobs = []

for line in out.split('\n'):
    if re.search('PENDING', line):
        jobs += [line.split()[0]]

for job in jobs:
    p = subprocess.Popen(['scancel', '{}'.format(job)],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
