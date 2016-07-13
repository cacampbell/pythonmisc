#!/usr/bin/env python3
from ParallelCommand import ParallelCommand


class UnzipAll(ParallelCommand):
    def __init__(self, *args, **kwargs):
        super(UnzipAll, self).__init__(*args, **kwargs)
        self.input_regex = ".*\.zip$|.*\.gz$|.*\.bz2$"
        self.extension = ".*"
        self.set_default('cluster_options', {'memory': "2G",
                                             'cpus': '2',
                                             'job_name': 'Zipper_',
                                             'partition': 'bigmemm'})

    def __zip_command(self, filename):
        output = self.rebase_file(filename)
        output = output.rstrip(".zip")
        command = ("unzip -c {} > {}").format(filename, output)
        return (command)

    def __gz_command(self, filename):
        output = self.rebase_file(filename)
        output = output.rstrip(".gz")
        command = ("gzip -d -c {} > {}").format(filename, output)
        return (command)

    def __bz2_command(self, filename):
        output = self.rebase_file(filename)
        output = output.rstrip(".bz2")
        command = ("bzip2 -d -c {} > {}").format(filename, output)
        return (command)

    def make_command(self, filename):
        if filename.endswith(".zip"):
            return self.__zip_command(filename)
        elif filename.endswith(".gz"):
            return self.__gz_command(filename)
        elif filename.endswith(".bz2"):
            return self.__bz2_command(filename)
