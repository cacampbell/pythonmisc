#!/usr/bin/env python
import subprocess
import sys
import unittest

import os

__all__ = ['module']


if 'MODULE_VERSION' not in os.environ:
    os.environ['MODULE_VERSION_STACK'] = '3.2.10'  # Just a guess
    os.environ['MODULE_VERSION'] = '3.2.10'  # Most recent when writing this
else:
    os.environ['MODULE_VERSION_STACK'] = os.environ['MODULE_VERSION']

if 'LOADEDMODULES' not in os.environ:
    os.environ['LOADEDMODULES'] = ''

os.environ['MODULESHOME'] = '/usr/share/modules'

if 'MODULEPATH' not in os.environ:
    with open(os.path.join(os.environ['MODULESHOME'],
                           'init/.modulespath'), "r") as modulespath_handle:
        path = []

        for line in modulespath_handle.readlines():
            if not line.startswith("#"):
                path.append(line)

        os.environ['MODULEPATH'] = ':'.join(path)


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
        process = subprocess.Popen(['/usr/bin/modulecmd', 'python'] + args,
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
        with unittest.TestCase.assertRaises(Exception):
            module({'wrong': "bad"})
        module('load', 'python')
        module('unload', 'python')
        module('avail')
        module('list')


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(test_module_loader)
    unittest.TextTestRunner(verbosity=3).run(suite)
