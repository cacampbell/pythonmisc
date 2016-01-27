#!/usr/bin/env python
from getpass import getuser
from clusterlib.scheduler import submit
from clusterlib.scheduler import queued_or_running_jobs
import os
import re
from subprocess import call
import sys
from uuid import uuid4


class BAM2SAM:
    def __init__(self, directory, threads, slurm_logs=None, test=False):
        self.directory = directory
        self.threads = threads
        self.memory = 8000 * threads
        self.test = test
        self.user = getuser()
        self.cluster_partition = "bigmemm"
        self.slurm_logs = slurm_logs or self.directory

    def get_bam_files(self):
        sys.stdout.write("Gathering files...\n")
        files = []

        for lane_file in os.listdir(self.directory):
            absfile = os.path.abspath(os.path.join(self.directory, lane_file))
            try:
                assert(absfile.endswith(".bam"))
                assert(os.stat(absfile).st_size != 0)
                files.append(absfile)
            except AssertionError as bad_assert:
                sys.stderr.write("%s:\n This file does not have the correct" \
                                 % absfile + " extension (not *.bam), or this" +
                                 " file has size 0 (it is empty). Ignoring" +
                                 " this file.\n")

        if not files == []:
            return files
        else:
            sys.stderr.write("No suitable files found. Cannot proceed.\n")
            sys.exit(2)

    def make_commands(self, bam_files):
        sys.stdout.write("Formatting Commands...\n")
        cmds = []

        for lane_file in bam_files:
            samfile = os.path.basename(lane_file).replace(".bam", ".sam")
            output_file = os.path.join(os.path.split(lane_file)[0], samfile)
            cmd = "samtools view -h -o %s -@ %s %s" \
                                % (output_file, self.threads, lane_file)

            cmds.append(cmd)

            if self.test:
                sys.stdout.write(cmd + "\n")

        return cmds

    def __random_job_name(self):
        return "bam2sam_%s" % uuid4()

    def __get_job_name(self, cmd):
        r = "(?<=\W)/.*\.bam$"
        m = re.search(r, cmd)

        if m:
            job_name = None

            if self.test:
                sys.stdout.write("Matched, using filename for job.\n")

            try:
                filename = os.path.basename(m.group(0))
                job_name = "bam2sam_%s" % filename
            except Exception as e:
                sys.stderr.write("Could not name job based on " +
                                 "extracted file name.\n Exception: %s.\n " +
                                 "Using random job_name.\n" % str(e))
                job_name = self.__random_job_name()
            finally:
                return job_name
        else:
            if self.test:
                sys.stdout.write("Did not match filename. Using random uuid\n")

            return self.__random_job_name()


    def make_scripts(self, cmds, job_names=None):
        sys.stdout.write("Writing Scripts...\n")
        scheduled_jobs = set(queued_or_running_jobs(user=self.user))
        scripts = []

        if job_names == None:
            job_names = []

            for cmd in cmds:
                job_names.append(None)


        for index, cmd in enumerate(cmds):
            job_name = job_names[index] or self.__get_job_name(cmd)
            if job_name not in scheduled_jobs:
                script = submit(cmd,
                                memory=self.memory,
                                job_name=job_name,
                                log_directory=self.slurm_logs,
                                backend='slurm',
                                time=0,
                                shell_script="#!/usr/bin/env bash")
                script = script + " --cpus-per-task=%s" % self.threads
                script = script + " --partition=%s" % self.cluster_partition
                scripts.append(script)

                if self.test:
                    sys.stdout.write(script + "\n")

        return scripts

    def dispatch(self, scripts):
        sys.stdout.write("Dispatching to Slurm...\n")

        for script in scripts:
            if not self.test:
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
