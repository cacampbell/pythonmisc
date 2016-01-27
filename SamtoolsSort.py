#!/usr/bin/env python
from getpass import getuser
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
import os
import re
from subprocess import call
import sys
from uuid import uuid4

class SamtoolsSort:
    def __init__(self, directory, threads, slurm_logs=None, verbose=False,
                 memory=None):
        self.directory = directory
        self.threads = threads
        self.mem_per_cpu = 8
        self.verbose = verbose
        self.user = getuser()
        self.cluster_partition = "bigmemm"
        self.slurm_logs = slurm_logs or self.directory
        self.memory = None
        if memory:
            self.memory = memory * 1000
        else:
            self.memory = self.threads * self.mem_per_cpu * 1000

    def get_bam_files(self):
        sys.stdout.write("Gathering files...\n")
        files = []

        for lane_file in os.listdir(self.directory):
            absfile = os.path.abspath(os.path.join(self.directory, lane_file))

            try:
                assert(absfile.endswith(".bam"))
                assert(not os.path.isdir(absfile))
                assert(os.stat(absfile).st_size != 0)
                files.append(absfile)
            except AssertionError:
                sys.stderr.write("%s:\nIncorrect Format. Skipping this file.\n"\
                                 % lane_file)

        if not files == []:
            return files
        else:
            sys.stderr.write("No suitable files found. Cannot proceed.\n")
            sys.exit(2)

    def __make_temp_directory(self, directory):
        try:
            if self.verbose:
                sys.stdout.write("Temp Dir: %s\n" % directory)
            else:
                os.makedirs(directory)
        except OSError as e:
            sys.stderr.write("Could not make temp directory: %s\n" % str(e))

    def make_commands(self, files):
        sys.stdout.write("Formating Commands...\n")
        cmds = []

        for lane_file in files:
            filename = os.path.basename(lane_file)
            output_file = os.path.join(self.directory,
                                       filename.replace(".bam", ".sorted.bam"))
            temporary_directory = os.path.join(self.directory,
                                               ".%s" % \
                                               self.__get_job_name(filename))
            self.__make_temp_directory(temporary_directory)
            temporary_prefix = os.path.join(temporary_directory,
                                            "tmp_")
            cmd = "samtools sort -@ %s -m %sG -O bam -o %s -T %s -l 1 %s" \
                % (self.threads, self.mem_per_cpu, output_file, temporary_prefix,
                   lane_file)

            cmds.append(cmd)

            if self.verbose:
                sys.stdout.write(cmd + "\n")

        return cmds

    def __random_job_name(self):
        return "%s" % uuid4()

    def __get_job_name(self, cmd):
        r = "(?<=[^|\W])/?.+\.bam(?=[\W]|$)"
        m = re.search(r, cmd)

        if m:
            job_name = None

            if self.verbose:
                sys.stdout.write("Matched, using filename for job.\n")

            try:
                filename = os.path.basename(m.group(0))
                job_name = "samtools_sort_%s" % os.path.splitext(filename)[0]
            except Exception as e:
                sys.stderr.write("Could not name job based on " +
                                 "extracted file name.\n Exception: %s.\n " +
                                 "Using random job_name.\n" % str(e))
                job_name = self.__random_job_name()
            finally:
                return job_name
        else:
            if self.verbose:
                sys.stdout.write("Did not match filename. Using random uuid\n")

            return self.__random_job_name()

    def make_scripts(self, cmds, job_names=None):
        sys.stdout.write("Writing Scripts...\n")
        scheduled_jobs = set(queued_or_running_jobs(user=self.user))
        scripts = []

        if job_names is None:
            job_names = []

            for cmd in cmds:
                job_names.append(None)

        for index, cmd in enumerate(cmds):
            job_name = job_names[index] or self.__get_job_name(cmd)
            if job_name not in scheduled_jobs:
                script = submit(cmd,
                                memory="%s" % self.memory,
                                job_name=job_name,
                                log_directory=self.slurm_logs,
                                backend='slurm',
                                time=0,
                                shell_script="#!/usr/bin/env bash")
                script = script + " --partition=%s" % self.cluster_partition
                script = script + " --cpus-per-task=%s" % self.threads
                scripts.append(script)

                if self.verbose:
                    sys.stdout.write(script + "\n")

        return scripts

    def dispatch(self, scripts):
        sys.stdout.write("Dispatching to Slurm...\n")

        for script in scripts:
            if not self.verbose:
                call(script, shell=True)
            else:
                sys.stdout.write("Would have dispatched:\n%s\n" % script)

    def run(self):
        try:
            files = self.get_bam_files()
            cmds = self.make_commands(files)
            scripts = self.make_scripts(cmds)
            self.dispatch(scripts)
        except Exception as e:
            sys.stderr.write("Unexpected Exception while running! %s\n"
                             % str(e))
