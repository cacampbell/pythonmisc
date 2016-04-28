#!/usr/bin/env python
from Bash import bash
from module_loader import module

module('load', 'torque', 'maui')


def qsub(command, *args):
    script = ("echo '#!/usr/bin/env bash\n {}' | qsub".format(command))

    for argument in args:
        script += " {}".format(argument)

    return (bash(script))


def qstat(*args):
    return (bash("qstat", *args))


def qjob(job):
    return (bash("qstat -j", job))


def qdel(*args):
    return (bash("qdel", *args))


def qalter(*args):
    return (bash("qalter", *args))


def qresub(*args):
    return (bash("qresub", *args))
