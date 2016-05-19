import re

from ParallelCommand import ParallelCommand


class AdapterFinder(ParallelCommand):
    def __init__(self, input_root, output_root):
        self.read_marker = "_R1"
        self.mate_marker = "_R2"
        self.reference = "reference.fa"
        super(AdapterFinder, self).__init__(input_root, output_root)

    def make_command(self, filename):
        mate = re.sub(self.read_marker, self.mate_marker, filename)
        adapter = re.sub(self.read_marker, "_Adapters", filename)
        adapter = re.sub(self.input_suffix, ".fa", adapter)
        adapter = self.rebase_file(adapter)
        command = ("bbmerge.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} "
                   "outa={o}").format(xmx=self.get_mem(fraction=0.95),
                                      t=self.get_threads(), i1=filename,
                                      i2=mate, o=adapter)
        return command


class AdapterFinder
