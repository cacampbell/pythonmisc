#!/usr/bin/env python
import sys
import os
import re

filebases = []
with open('failed_files.txt', 'r+') as h:
    lines = (line.rstrip() for line in h)
    lines = list(line for line in lines if line)
    for line in lines:
        tokens = line.split(":")
        path = tokens[1]
        filename = os.path.basename(path)
        filename = re.sub('.fq.gz|.human.fq.gz', '', filename)
        filebases += [filename]

unique_bases = set(filebases)
sys.stdout.write('[')
for filename in unique_bases:
    sys.stdout.write("{}.fq.gz, ".format(filename))
sys.stdout.write(']\n')

