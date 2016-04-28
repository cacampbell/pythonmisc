#!/usr/bin/env python
from __future__ import print_function

from Bash import bash
from module_loader import module

module('load', 'slurm')  # Import slurm from environment module system


def sbatch(command, *args):
    script = "echo '#!/usr/bin/env bash\n {}' | sbatch".format(command)

    for argument in args:
        script += " {}".format(argument)

    return (bash(script))


def squeue(*args):
    return (bash("squeue", *args))


def scancel(*args):
    return (bash("scancel", *args))


def scontrol(*args):
    return (bash("scontrol", *args))


def sjob(job):
    return (scontrol(["show", job]))
