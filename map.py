#!/usr/bin/env python3
from BBToolsMap import BBMapper
from BBToolsMap_NoStats import BBMapperNoStats
from BBWrapper import BBWrapper
from BwaMemMap import BWAMEM
from parallel_command_parse import run_parallel_command_with_args


class MapperFactory:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def get_mapper(self):
        if 'bwamem' in self.kwargs:
            return (BWAMEM(*self.args, **self.kwargs))

        # Stats takes precedence over wrap
        if 'stats' in self.kwargs:  # --stats : desired mapping statistics
            if self.kwargs['stats']:
                return(BBMapper(*self.args, **self.kwargs))

        # Stats and Wrap are mutually exclusive -- cannot get mapping stats
        # if using the bbwrap.sh script
        if 'wrap' in self.kwargs:  # --wrap : use index one time only
            if self.kwargs['wrap']:
                if 'read_groups' in self.kwargs:
                    if self.kwargs['read_groups']:
                        print("Cannot add read groups with wrapper")
                        return (BBMapperNoStats(*self.args, **self.kwargs))

                return(BBWrapper(*self.args, **self.kwargs))
        else:
            return(BBMapperNoStats(*self.args, **self.kwargs))


def main(*args, **kwargs):
    bb = MapperFactory(*args, **kwargs)
    mapper = bb.get_mapper()
    mapper.modules = ['java', 'samtools', 'bwa']
    return(mapper.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
