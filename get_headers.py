#!/usr/bin/env python
from __future__ import print_function

with open('sample_fastqc_data.txt', 'r+') as file_h:
    lines = file_h.readlines()
    for line in lines:
        if line.startswith('>>') and not line.startswith('>>END_MODULE'):
            print(line)
