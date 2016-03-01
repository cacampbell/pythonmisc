#!/usr/bin/env python
import sys
import os
import subprocess

input_dir = sys.argv[1] or os.getcwd()
output_dir = sys.argv[2] or os.getcwd()
commands = {}

for root, directories, filenames in os.walk(input_dir):
    for filename in filenames:
        if not filename.endswith('.gz'):
            job_name = 'gzip_{}'.format(filename)
            abs_path = os.path.join(root, filename)
            output_abs = os.path.join(output_dir, filename)
            commands[job_name] = 'gzip -c {inputfile} > {outputfile}.gz'.format(inputfile=abs_path, outputfile=output_abs)


for job, command in commands.iteritems():
    subprocess.call('echo "#!/usr/bin/env bash\n{cmd}" | sbatch --partition=bigmemh --ntasks=1 --cpus-per-task=2 --job-name={jobname}'.format(cmd=command, jobname=job),
                    shell=True)
