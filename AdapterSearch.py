from PairedEndCommand import PairedEndCommand


class AdapterFinder(PairedEndCommand):
    def __init__(self, *args, **kwargs):
        super(AdapterFinder, self).__init__(*args, **kwargs)
        # self.read_regex = "_R1(?=\.fq)"

    def make_command(self, filename):
        mate = self.mate(filename)
        adapter = self.replace_read_marker_with("_Adapter", filename)
        adapter = self.replace_extension(".fa", adapter)
        adapter = self.rebase_file(adapter)
        command = ("bbmerge.sh -Xmx{xmx} threads={t} in1={i1} in2={i2} "
                   "outa={o}").format(xmx=self.get_mem(fraction=0.95),
                                      t=self.get_threads(),
                                      i1=filename,
                                      i2=mate,
                                      o=adapter)
        return (command)
