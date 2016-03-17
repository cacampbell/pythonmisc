#!/usr/bin/env python
from __future__ import print_function
import subprocess
import unittest


def __bash_command(command, *args):
    command = [command]

    if args:
        for argument in args:
            command.extend(argument)

    try:
        process = subprocess.Popen(command,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (out, err) = process.communicate()
        output = out.decode('utf-8').splitlines()
        error = err.decode('utf-8').splitlines()
        return (output, error)

    except (OSError, subprocess.CalledProcessError) as err:
        print("Encountered error while calling command: {}\n{}".format(command,
                                                                       err))
        raise err


def squeue(*args):
    command = 'squeue'
    __bash_command(command, args)


def sbatch(command, *args):
    sb = "sbatch {} ".format(command)

    if args:
        for arg in args:
            sb += arg

    script = "echo '#!/usr/bin/env bash\n{}'".format(sb)
    __bash_command(script)


def scontrol(*args):
    command = "scontrol"
    __bash_command(command, args)


class TestSlurmCommands(unittest.TestCase):
    def test___bash_command():
        pass

    def test_squeue():
        pass

    def test_sbatch():
        pass

    def test_scontrol():
        pass


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSlurmCommands)
    unittest.TextTestRunner(verbosity=3).run(suite)
