#!/usr/bin/env python
from __future__ import print_function
import os
import sys
from subprocess import call
import errno
from module_loader import module
from clusterlib.scheduler import queued_or_running_jobs
from clusterlib.scheduler import submit
import unittest
from abc import abstractmethod
from abc import ABCMeta


class ParallelCommand:
    __metaclass__ = ABCMeta

    @staticmethod
    def rebase(filename, src_root, dest_root):
        return os.path.join(dest_root, os.path.relpath(filename,
                                                       start=src_root))

    def output_file(self, filename):
        return ParallelCommand.rebase(filename,
                                      self.input_root,
                                      self.output_root)

    @staticmethod
    def mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                print("{} already exists".format(path))
            else:
                raise exc

    def __init__(self, input_=os.getcwd(), output_=os.getcwd()):
        self.input_root = input_
        self.output_root = output_
        self.slurm_options = {
            'mem': '200G',
            'tasks': '1',
            'cpus': '24',
            'job-name': 'Dispatch',
            'time': '0',
            'mail-user': 'example@example.com',
            'mail-type': 'END,FAIL',
            'partition': 'bigmemm'
        }
        self.modules = ['']
        self.job_prefix = ''
        self.input_suffix = ''
        self.dry_run = True
        self.verbose = True
        self.read_marker = "R1"
        self.mate_marker = "R2"
        self.reference = ""
        self._files = []
        self._commands = {}
        self._scripts = {}

    def get_threads(self):
        return str(int(self.slurm_options['cpus']) * 4 - 2)

    def get_mem(self, fraction=1):
        assert(float(fraction) <= 1.0)
        mem_int = float(self.slurm_options['mem'][:-1])
        mem_unit = self.slurm_options['mem'][-1]
        memory = float(mem_int) * float(fraction)
        memory = int(memory)  # Must be int for slurm version on farm
        return "{}{}".format(memory, mem_unit)

    def dispatch(self):
        scheduled_jobs = set(queued_or_running_jobs())
        for job_name, script in self._scripts.iteritems():
            if job_name not in scheduled_jobs:
                if self.verbose:
                    print("Dispatching {}...".format(job_name), file=sys.stderr)
                if not self.dry_run:
                    call(script, shell=True)
            else:
                print("{} already running, skipping...", file=sys.stderr)

    def scripts(self):
        for job_name, command in self._commands.iteritems():
            script = submit(command,
                            job_name=job_name or self.slurm_options['job_name'],
                            time=self.slurm_options['time'],
                            memory=self.slurm_options['mem'],
                            backend='slurm',
                            shell_script='#!/usr/bin/env bash')
            script += " --partition={}".format(self.slurm_options['partition'])
            script += " --ntasks={}".format(self.slurm_options['tasks'])
            script += " --cpus-per-task={}".format(self.slurm_options['cpus'])
            script += " --mail-user={}".format(self.slurm_options['mail-user'])
            script += " --mail-type={}".format(self.slurm_options['mail-type'])
            self._scripts[job_name] = script

            if self.verbose:
                print(script)

    @abstractmethod
    def make_command(self, read):
        pass

    def commands(self):
        for read in self._files:
            job_name = "{}{}".format(self.job_prefix, os.path.basename(read))
            command = self.make_command(read)
            assert(type(command) is str)
            self._commands[job_name] = command
            if self.verbose:
                print(command)

    def get_files(self):
        for root, _, files in os.walk(self.input_root):
            for filename in files:
                if filename.endswith(self.input_suffix):
                    if self.read_marker in filename:
                        abs_path = os.path.join(root, filename)
                        self._files += [abs_path]

                        if self.verbose:
                            print(abs_path, file=sys.stderr)

    def load_modules(self):
        try:
            args = ['load']
            args.extend(self.modules)
            module(args)
        except Exception as e:
            print("Could not load module(s):{}, {}".format(self.modules, e))

    def make_directories(self):
        directories = [x[0] for x in os.walk(self.input_root)]
        output_directories = [self.rebase(x, self.input_root, self.output_root)
                              for x in directories]

        for directory in output_directories:
            if self.verbose:
                print("Attempting to make: {}".format(directory))
            if not self.dry_run:
                self.mkdir_p(directory)

    def run(self):
        if self.verbose:
            print('Loading environment modules...', file=sys.stderr)
        self.load_modules()

        if self.verbose:
            print('Gathering input files...', file=sys.stderr)
        self.get_files()

        if self.verbose:
            print("Making output directories...", file=sys.stderr)
        self.make_directories()

        if self.verbose:
            print('Writing commands...', file=sys.stderr)
        self.commands()

        if self.verbose:
            print('Writing scripts...', file=sys.stderr)
        self.scripts()

        if self.verbose:
            print('Dispatching scripts to slurm...', file=sys.stderr)
        self.dispatch()


class TestParallelCommand(unittest.TestCase):
    def setUp(self):
        pass

    def test_make_directories(self):
        pass

    def test_load_modules(self):
        pass

    def test_get_files(self):
        pass

    def scripts(self):
        pass

    def dispatch(self):
        pass

    def test_run(self):
        pass

    def tearDown(self):
        pass


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestParallelCommand)
    unittest.TextTestRunner(verbosity=3).run(suite)
