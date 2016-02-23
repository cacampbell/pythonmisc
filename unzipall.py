#!/usr/bin/env python
import os
import subprocess
import re


input_dir = '/group/nealedata5/Psme_reseq'
commands = {}


for root, directories, filenames in os.walk(input_dir):
    for filename in filenames:
        if filename.endswith('.gz'):
            job_name = 'guzip_{}'.format(filename)
            abs_path = os.path.join(root, filename)
            output = os.path.join(root, filename)
            output_abs = re.sub('.gz', '', output)
            commands[job_name] = 'gunzip -c {inputfile} > {outputfile}'.format(inputfile=abs_path, outputfile=output_abs)


for job, command in commands.iteritems():
    subprocess.call('echo "#!/usr/bin/env bash\n{cmd}" | sbatch --partition=bigmemm --ntasks=1 --cpus-per-task=2 --job-name={jobname}'.format(cmd=command, jobname=job),
                    shell=True)
