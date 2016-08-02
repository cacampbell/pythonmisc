#!/usr/bin/env python3
from sys import stderr

from BBToolsMap import BBMapper
from BBWrapper import BBWrapper
from BwaMemMap import BWAMEM
from parallel_command_parse import run_parallel_command_with_args


class MapperFactory:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def get_mapper(self):
        if 'bwamem' in self.kwargs.keys():
            return (BWAMEM(*self.args, **self.kwargs))

        # Stats and Wrap are mutually exclusive -- cannot get mapping stats
        # if using the bbwrap.sh script
        if 'wrap' in self.kwargs.keys():  # --wrap : use index one time only
            if self.kwargs['wrap']:
                if 'read_groups' in self.kwargs.keys() or \
                                'stats' in self.kwargs.keys():
                    print("BBWrapper cannot assign read groups or print stats",
                          file=stderr)
                    raise (RuntimeError("Contradictory options"))

                return(BBWrapper(*self.args, **self.kwargs))
        else:
            return (BBMapper(*self.args, **self.kwargs))


def main(*args, **kwargs):
    bb = MapperFactory(*args, **kwargs)
    mapper = bb.get_mapper()
    mapper.modules = ['java', 'samtools', 'bwa']
    return(mapper.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
