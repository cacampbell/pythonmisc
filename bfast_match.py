#!/usr/bin/env python
from __future__ import print_function
import os
import subprocess
import re

input_directory = "/home/smontana/P.communis_trimmed_reads"
dry_run = False

for filename in os.listdir(input_directory):
    if filename.endswith("_qual.adap_trimmed.fastq"):
        print(filename)
        abs_file = os.path.join(input_directory, filename)
        output_file = re.sub("_qual.adap_trimmed.fastq", ".bmf", abs_file)

        if not os.path.isfile(output_file) or os.stat(output_file).st_size == 0:
            command = ("bfast match -f /group/nealedata4/pear_diversity/"
                       "genome/Pyrus_communis_v1.0-scaffolds"
                       ".fna -A 0 -n 2 -r {} > {}").format(abs_file,
                                                           output_file)
            command = re.sub('\n', '', command)
            script = "echo '#!/usr/bin/env bash\n{}'".format(command)
            dispatch = ("{} | sbatch --partition=bigmemh --job-name=bfast"
                        " --ntasks=1 --cpus-per-task=2 "
                        "--mem=20G").format(script)
            if not dry_run:
                subprocess.call(dispatch, shell=True)
            else:
                print(dispatch)
