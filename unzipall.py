#!/usr/bin/env python
import os
import subprocess
import re


input_dir = '/group/nealedata4/Psme_reseq/fastq'
output_dir = '/group/nealedata4/Psme_reseq/fastq'
commands = {}


for root, directories, filenames in os.walk(input_dir):
    for filename in filenames:
        if filename.endswith('.gz'):
            job_name = 'guzip_{}'.format(filename)
            abs_path = os.path.join(root, filename)
            output_abs = os.path.join(output_dir, filename)
            re.sub('.gz', '', output_abs)
            commands[job_name] = 'gunzip -c {inputfile} > {outputfile}'.format(inputfile=abs_path, outputfile=output_abs)


for job, command in commands.iteritems():
    subprocess.call('echo "#!/usr/bin/env bash\n{cmd}" | sbatch --partition=bigmemh --ntasks=1 --cpus-per-task=2 --job-name={jobname}'.format(cmd=command, jobname=job),
                    shell=True)
