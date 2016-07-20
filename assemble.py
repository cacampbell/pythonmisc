#!/usr/bin/env python3
from AbySSAssemble import AbySSAssemble
from SOAPdenovoAssemble import SOAPdenovoAssemble
from TadpoleAssemble import TadpoleAssemble
from TrinityAssemble import TrinityAssemble
from VelvetOasesAssemble import VelvetOasesAssemble
from parallel_command_parse import run_parallel_command_with_args


class AssemblyFactory:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.ASSEMBLER = {
            'tadpole': self.tadpole_assemble,
            'trinity': self.trinity_assemble,
            'velvet': self.velvet_oases_assemble,
            'oases': self.velvet_oases_assemble,
            'velvet_oases': self.velvet_oases_assemble,
            'soapdenovo': self.soap_denovo_assemble,
            'abyss': self.abyss_assemble
        }

    def tadpole_assemble(self):
        assembler = TadpoleAssemble(*self.args, **self.kwargs)
        assembler.input_regex = ".*"
        assembler.read_regex = ".*"
        # assembler.extension = r".fq.gz"
        assembler.modules = ['java/1.8', 'slurm']
        return (assembler)

    def trinity_assemble(self):
        if "mode" in self.kwargs.keys():
            if not self.kwargs["mode"] == "RNA":
                raise (RuntimeError("Wrong mode for Trinity, use --mode=RNA"))

        else:
            print("Assuming input sequences are transcriptomic data")

        assembler = TrinityAssemble(*self.args, **self.kwargs)
        assembler.modules = ["java", "trinity"]

        if "genome_guided" in self.kwargs.keys():
            assembler.modules = ["java", "samtools", "trinity"]

        return (assembler)

    def velvet_oases_assemble(self):
        assembler = VelvetOasesAssemble(*self.args, **self.kwargs)
        return (assembler)

    def soap_denovo_assemble(self):
        assembler = SOAPdenovoAssemble(*self.args, **self.kwargs)
        return (assembler)

    def abyss_assemble(self):
        assembler = AbySSAssemble(*self.args, **self.kwargs)
        return (assembler)

    def get_assembler(self):
        if not "assembler" in self.kwargs.keys():
            raise RuntimeError("No Assembler specified: use --assembler=")

        if not "mode" in self.kwargs.keys():
            if not self.kwargs["assembler"].lower().strip() in ["trinity",
                                                                "tadpole",
                                                                "masurca"]:
                raise RuntimeError("Please Specify Mode: --mode=DNA|RNA")

        # self.ASSEMBLER is a dictionary, with keys = the different possible
        # assemblers that can be invoked from this script. So, we access
        # the key of ASSEMBLER that matches the given assembler argument
        # (stripped and lowercase), then immediately call it, return the
        # result
        return (self.ASSEMBLER[self.kwargs["assembler"].lower().strip()]())


def main(*args, **kwargs):
    assembler = AssemblyFactory(*args, **kwargs).get_assembler()
    assembler.input_regex = ".*"
    assembler.read_regex = ".*"
    assembler.extension = ".fq"

    if "genome_guided" in kwargs.keys():
        assembler.extension = ".bam"

    return (assembler.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
