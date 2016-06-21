#!/usr/bin/env python3
from BBToolsMap import BBMapper
from BBWrapper import BBWrapper
from BBToolsMap_NoStats import BBMapperNoStats
from simple_argparse import run_parallel_command_with_args


class BBTools_factory:
    def __init__(*args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def get_mapper():
        # Stats takes precedence over wrap
        if 'stats' in self.kwargs:  # --stats : desired mapping statistics
            if kwargs['stats']:
                return(BBMapper(*self.args, **self.kwargs))

        # Stats and Wrap are mutually exclusive -- cannot get mapping stats
        # if using the bbwrap.sh script
        if 'wrap' in kwargs:  # --wrap : use index one time only
            if kwargs['wrap']:
                return(BBWrapper(*self.args, **self.kwargs))
        else:
            return(BBMapperNoStats(*self.args, **self.kwargs))


def main(*args, **kwargs):
    bb = BBTools_factory(*args, **kwargs)
    mapper = bb.get_mapper()
    return(mapper.run())


if __name__ == "__main__":
    run_parallel_command_with_args(main)
    print(jobs)
