from ParallelCommand import ParallelCommand
import re


class AdapterFinder(ParallelCommand):
    def make_command(self, filename):
        mate = re.sub(self.read_marker, self.mate_marker, filename)
        adapter = re.sub(self.read_marker, "_Adapters", filename)
        adapter = re.sub(self.input_suffix, ".fa", adapter)
        adapter = self.output_file(adapter)
        command = ("bbmerge.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} "
                   "outa={o}").format(xmx=self.get_mem(fraction=0.95),
                                      t=self.get_threads(), i1=filename,
                                      i2=mate, o=adapter)
        return command