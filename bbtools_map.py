#!/usr/bin/env python3
from BBToolsMap import BBMapper
from BBToolsMap_NoStats import BBMapperNoStats
from BBWrapper import BBWrapper
from parallel_command_parse import run_parallel_command_with_args


class BBTools_factory:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def get_mapper(self):
        # Stats takes precedence over wrap
        if 'stats' in self.kwargs:  # --stats : desired mapping statistics
            if self.kwargs['stats']:
                return(BBMapper(*self.args, **self.kwargs))

        # Stats and Wrap are mutually exclusive -- cannot get mapping stats
        # if using the bbwrap.sh script
        if 'wrap' in self.kwargs:  # --wrap : use index one time only
            if self.kwargs['wrap']:
                return(BBWrapper(*self.args, **self.kwargs))
        else:
            return(BBMapperNoStats(*self.args, **self.kwargs))


def main(*args, **kwargs):
    bb = BBTools_factory(*args, **kwargs)
    mapper = bb.get_mapper()
    mapper.modules = ['java', 'samtools']
    return(mapper.run())


if __name__ == "__main__":
    jobs = run_parallel_command_with_args(main)
    print(jobs)
