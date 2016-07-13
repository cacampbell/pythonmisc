#!/usr/bin/env python3
import subprocess
import sys
import unittest

import os  # Whole module needed due to 'exec(out)' from env module system
from re import findall

from Bash import which

__all__ = ['module']


def __find_module_cmd():
    """
    Return first path that leads to a module command, or exit with no module
    loader found in the current environment.
    """
    module_cmd = which("modulecmd")
    if module_cmd:
        lines = module_cmd.splitlines()
        if lines[0].startswith("alias"):
            return (findall("([^']*)", lines[0]))
        else:
            return (lines[0])
    else:
        raise (RuntimeError("No 'modulecmd' found in current environment"))

def module(*args):
    if type(args[0]) is list:
        args = args[0]
    else:
        args = list(args)

    try:
        # /usr/bin/modulecmd allows specification of the shell as the first
        # argument, then executes the module command from that shell. Here,
        # the shell is python.
        # From the python shell, the arguments passed to this function are
        # command line options for the module loader
        # Thus, passing 'load', 'modulename', becomes /usr/bin/modulecmd python
        # load modulename. This loads the module
        # Note that loading modules results in output printed to stderr
        if not "PYMODULECMD" in os.environ:
            os.environ["PYMODULECMD"] = __find_module_cmd()

        module_cmd = os.environ["PYMODULECMD"]
        process = subprocess.Popen([module_cmd, 'python'] + args,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (out, err) = process.communicate()
        exec(out)

    except (OSError, ValueError) as e:
        sys.stdout.write("Module load execution: {0:s}\n".format(e))


class test_module_loader(unittest.TestCase):
    def test_module(self):
        # test module format_commands as they might be called in a bash shell, using
        # the module function from this python module
        with self.assertRaises(Exception):
            module({'wrong': "bad"})
        module('load', 'python')
        module('unload', 'python')
        module('avail')
        module('list')


if __name__ == "__main__":
    module_cmd = __find_module_cmd()
    suite = unittest.TestLoader().loadTestsFromTestCase(test_module_loader)
    unittest.TextTestRunner(verbosity=3).run(suite)
