#!/usr/bin/env python3
from AbySSAssemble import AbySSAssemble
from OasesAssemble import OasesAssemble
from SOAPdenovoAssemble import SOAPdenovoAssemble
from TadpoleAssemble import TadpoleAssemble
from TrinityAssemble import TrinityAssemble
from VelvetAssemble import VelvetAssemble
from parallel_command_parse import run_parallel_command_with_args


# TODO: multi_assemble, metassembler -- can metaseembler combine de novo and ref
# guided assemblies? Or do I need to use tr2aacds to combine ref guided and
# de novo assemblies? Different software for transcriptome vs genome assemblies?

# TODO: why do I set parameters in the assembly classes using set default,
# but then also set the parameters for the assembler object after creation,
# both during the factory method and during main? what?

class AssemblyFactory:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.ASSEMBLER = {
            'tadpole': self.tadpole_assemble,
            'trinity': self.trinity_assemble,
            'velvet': self.velvet_assemble,
            'oases': self.oases_assemble,
            'soapdenovo': self.soap_denovo_assemble,
            'abyss': self.abyss_assemble
        }

    def tadpole_assemble(self):
        assembler = TadpoleAssemble(*self.args, **self.kwargs)
        assembler.modules = ['java', 'slurm']
        return (assembler)

    def trinity_assemble(self):
        assembler = TrinityAssemble(*self.args, **self.kwargs)
        assembler.modules = ["java", "trinity"]

        if "genome_guided" in self.kwargs.keys():
            assembler.modules = ["java", "samtools", "trinity"]

        return (assembler)

    def velvet_assemble(self):
        assembler = VelvetAssemble(*self.args, **self.kwargs)
        assembler.modules = ['boost', 'openmpi', 'sparsehash', 'abyss']
        return (assembler)

    def oases_assemble(self):
        assembler = OasesAssemble(*self.args, **self.kwargs)
        return (assembler)

    def soap_denovo_assemble(self):
        assembler = SOAPdenovoAssemble(*self.args, **self.kwargs)
        return (assembler)

    def abyss_assemble(self):
        assembler = AbySSAssemble(*self.args, **self.kwargs)
        return (assembler)

    def get_assembler(self):
        # self.ASSEMBLER is a dictionary, with keys = the different possible
        # assemblers that can be invoked from this script. So, we access
        # the key of ASSEMBLER that matches the given assembler argument
        # (stripped and lowercase), then immediately call it, return the
        # result
        return (self.ASSEMBLER[self.kwargs["assembler"].lower().strip()]())


def check_arguments(*args, **kwargs):
    assert ("mode" in kwargs.keys())
    assert ("assembler" in kwargs.keys())

    if kwargs["mode"].upper().strip() == "RNA":
        assert (kwargs["assembler"].lower().strip() not in ["velvet"])

    if kwargs["mode"].upper().strip() == "DNA":
        assert (kwargs["assembler"].lower().strip() not in ["trinity", "oases"])

    if "genome_guided" in kwargs.keys():
        assert (kwargs["assembler"].lower().strip() != "tadpole")


def main(*args, **kwargs):
    check_arguments(*args, **kwargs)
    assembler = AssemblyFactory(*args, **kwargs).get_assembler()
    return (assembler.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
