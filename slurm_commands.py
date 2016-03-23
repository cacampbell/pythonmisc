#!/usr/bin/env python
from __future__ import print_function
from module_loader import module
import subprocess
import shlex
import unittest
import errno
module('slurm')  # Import slurm from environment module system


def __bash_command(command, *args):
    cmd_args = [command]
    out = ""
    err = ""

    if len(args) == 1:
        cmd_args.extend(shlex.split(args[0]))
    else:
        for arg in args:
            cmd_args.extend(shlex.split(arg))
    try:
        process = subprocess.Popen(cmd_args,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (out, err) = process.communicate()
    except OSError as err:
        if err.errno == errno.ENOENT:  # No such file or directory
            process = subprocess.Popen(cmd_args,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       shell=True)  # Retry with subshell
            (out, err) = process.communicate()
        else:
            raise(err)
    except subprocess.CalledProcessError as err:
        print("Error while calling command: {}\n{}".format(command, err))
        raise(err)
    finally:
        return(out.decode('utf-8').splitlines(),
               err.decode('utf-8').splitlines())


def sbatch(command, *args):
    script = "echo '#!/usr/bin/env bash\n {}' | sbatch ".format(command)

    for argument in args:
        script += " {}".format(argument)

    return __bash_command(script)


def squeue(*args):
    return __bash_command("squeue", *args)


def scancel(*args):
    return __bash_command("scancel", *args)


class TestSlurmCommands(unittest.TestCase):
    def test___bash_command():
        pass


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSlurmCommands)
    unittest.TextTestRunner(verbosity=3).run(suite)
