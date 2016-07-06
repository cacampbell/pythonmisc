#!/usr/bin/env python3
from ParallelCommand import ParallelCommand


class ZipAll(ParallelCommand):
    def __init__(self, *args, **kwargs):
        super(ZipAll, self).__init__(*args, **kwargs)
        self.input_regex = ".*"
        self.extension = ".*"
        self.exclusions = ".*\.gz$"
        self.set_default('cluster_options', {'memory': "2G",
                                             'cpus': '2',
                                             'job_name': 'Zipper_',
                                             'partition': 'bigmemm'})

    def make_command(self, filename):
        output = self.rebase_file(filename)
        output += ".gz"
        command = ("gzip -c {} > {}").format(filename, output)
        return (command)
