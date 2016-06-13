"""
From Perlence/simple_argparse.py
"""
import unittest
from ast import literal_eval
from sys import argv
from sys import stderr

CLUSTER_OPTIONS = ("memory",
                   "nodes",
                   "cpus",
                   "partition",
                   "job_name",
                   "depends_on",
                   "email_address",
                   "email_options",
                   "time",
                   "bash")


def run_parallel_command_with_args(func, args=None):
    """
    Parse arguments from argv or given list of args, parse them for usage with
    the ParallelCommand / PairedEndCommand class,
    """

    def __str(val):
        if type(val) is list:
            return (
                ",".join(map(str, val)))  # Cluster options take lists as csv
        elif type(val) is int or type(val) is float:
            return (str(val))

    (args, kwargs) = parse_args(args)
    new_kwargs = dict()
    cluster_options = {}

    # If a cluster_options dict was supplied
    if "cluster_options" in kwargs.keys():
        cluster_options = kwargs["cluster_options"]

    # Find standalone cluster options provided by command line
    for (key, value) in kwargs.items():
        if key in CLUSTER_OPTIONS:
            # Cluster Options for Parallel Command are all strings
            cluster_options[key] = __str(value)

    # Remove keys in cluster options that are not supposed to be there
    cluster_options_copy = cluster_options.copy()
    for (key, value) in cluster_options_copy.items():
        if key not in CLUSTER_OPTIONS:
            cluster_options.pop(key)
            print("Removing Unexpected cluster option: {}".format(key),
                  file=stderr)

    if cluster_options != {}:
        new_kwargs["cluster_options"] = cluster_options

    for key in kwargs.keys():
        if key not in CLUSTER_OPTIONS and key is not "cluster_options":
            new_kwargs[key] = kwargs[key]

    return func(*args, **new_kwargs)


def run_with_args(func, args=None):
    """
    Parse arguments from argv or given list of args and pass them to *func*.
    """
    args, kwargs = parse_args(args)
    return func(*args, **kwargs)


def parse_args(args=None):
    """
    Parse positional arguments from the given list of arguments or from argv,
    and then parse keyword arguments from the given list or from argv
    :param: args: List<String>: optional: list of arguments as strings
    """
    if args is None:
        args = argv[1:]

    positional_args, kwargs = (), {}
    for arg in args:
        if arg.startswith('--'):
            arg = arg[2:]
            try:
                key, raw_value = arg.split('=', 1)
                value = parse_literal(raw_value)
            except ValueError:
                key = arg
                value = True
            kwargs[key.replace('-', '_')] = value
        else:
            positional_args += (parse_literal(arg),)

    return (positional_args, kwargs)


def parse_literal(string):
    """
    Parse Python literal or return *string* in case :func:`ast.literal_eval`
    fails
    """
    try:
        return literal_eval(string)
    except (ValueError, SyntaxError):
        return string


class TestArgParse(unittest.TestCase):
    def setUp(self):
        def test_func(*args, **kwargs):
            for arg in args:
                print("Arg: {}".format(arg))

            for (key, value) in kwargs.items():
                print("Key: {} Value: {}".format(key, value))

            return (args, kwargs)

        def evaluate_arguments(args, expected_args):
            # Assert that each argument in the expected arguments appears in
            # the actual arguments in order, and that it has the right value
            for (i, arg) in enumerate(expected_args):
                assert (arg == args[i])

            # Assert that the actual arguments list does not contain extras
            for arg in args:
                assert (arg in expected_args)

        def evaluate_keywords(kwargs, expected_dict):
            # Assert that each expected key appears in the actual kwargs
            # Assert that each value for each expected key is correct
            for (key, val) in expected_dict.items():
                assert (key in kwargs)
                assert (val == kwargs[key])

            # Assert that each key in the actual arguments is expected
            # This insures there are no extra keys
            for key in kwargs.keys():
                assert (key in expected_dict.keys())

        def evaluate(args, expected_args, kwargs, expected_kwargs):
            evaluate_arguments(args, expected_args)
            evaluate_keywords(kwargs, expected_kwargs)

        self.test_func = test_func
        self.evaluate = evaluate

    def test_0000(self):
        """
        positional arguments: 0
        keyword arguments: 0
        cluster_options_arguments: 0
        cluster_options_dictionary: 0
        """
        all = []
        expected_args = []
        expected_kwargs = {}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_0001(self):
        """
        positional arguments: 1
        keyword arguments: 0
        cluster_options_arguments: 0
        cluster_options_dictionary: 0
        """
        all = ["positional argument 1",
               "positional argument 2"]
        expected_args = ["positional argument 1",
                         "positional argument 2"]
        expected_kwargs = {}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_0011(self):
        """
        positional arguments: 1
        keyword arguments: 1
        cluster_options_arguments: 0
        cluster_options_dictionary: 0
        """
        all = ["positional argument 1",
               "positional argument 2",
               "--keyword-argument='non cluster keyword value'",
               "--foo=bar",
               "--baz={'a':'1', 'b':'2'}",
               "--expression=6 + 7"]
        expected_args = ["positional argument 1",
                         "positional argument 2"]
        expected_kwargs = {'keyword_argument': 'non cluster keyword value',
                           'foo': 'bar',
                           'baz': {'a': '1', 'b': '2'},
                           'expression': 13}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_0100(self):
        """
        positional arguments: 0
        keyword arguments: 0
        cluster_options_arguments: 1
        cluster_options_dictionary: 0
        """
        all = ["--memory=100G",
               "--nodes=1",
               "--cpus=20",
               "--partition=bigmem",
               "--job-name=JOBNAME",
               "--depends-on='1,2,3,4,5,6'",  # Important! NOT A LIST
               "--email-address=user@example.com",
               "--email-options=START,END,FAIL",
               "--time=24:00:00",
               "--bash=#!/bin/bash"]
        expected_args = []
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        }}
        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_0101(self):
        """
        positional arguments: 1
        keyword arguments: 0
        cluster_options_arguments: 1
        cluster_options_dictionary: 0
        """
        all = ["positional argument 1",
               "positional argument 2",
               "--memory=100G",
               "--nodes=1",
               "--cpus=20",
               "--partition=bigmem",
               "--job-name=JOBNAME",
               "--depends-on='1,2,3,4,5,6'",  # Important! NOT A LIST
               "--email-address=user@example.com",
               "--email-options=START,END,FAIL",
               "--time=24:00:00",
               "--bash=#!/bin/bash"
               ]
        expected_args = ["positional argument 1",
                         "positional argument 2"]
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        }}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_0110(self):
        """
        positional arguments: 0
        keyword arguments: 1
        cluster_options_arguments: 1
        cluster_options_dictionary: 0
        """
        all = ["--baz=5000",
               "--foo=bar",
               "--memory=100G",
               "--nodes=1",
               "--cpus=20",
               "--partition=bigmem",
               "--job-name=JOBNAME",
               "--depends-on='1,2,3,4,5,6'",  # Important! NOT A LIST
               "--email-address=user@example.com",
               "--email-options=START,END,FAIL",
               "--time=24:00:00",
               "--bash=#!/bin/bash"
               ]

        expected_args = []
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        },
            'foo': 'bar',
            'baz': 5000
        }

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_0111(self):
        """
        positional arguments: 1
        keyword arguments: 1
        cluster_options_arguments: 1
        cluster_options_dictionary: 0
        """
        all = ["positional argument 1",
               "positional argument 2",
               "--baz=5000",
               "--foo=bar",
               "--memory=100G",
               "--nodes=1",
               "--cpus=20",
               "--partition=bigmem",
               "--job-name=JOBNAME",
               "--depends-on='1,2,3,4,5,6'",  # Important! NOT A LIST
               "--email-address=user@example.com",
               "--email-options=START,END,FAIL",
               "--time=24:00:00",
               "--bash=#!/bin/bash"
               ]

        expected_args = ["positional argument 1",
                         "positional argument 2"]
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        },
            'foo': 'bar',
            'baz': 5000
        }

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1000(self):
        """
        positional arguments: 0
        keyword arguments: 0
        cluster_options_arguments: 0
        cluster_options_dictionary: 1
        """
        all = ["""--cluster-options={
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash',
            'fake': 'fake',
            'rattlesnake': 'death'}"""]
        expected_args = []
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        }
        }

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1001(self):
        """
        positional arguments: 1
        keyword arguments: 0
        cluster_options_arguments: 0
        cluster_options_dictionary: 1
        """
        all = ["positional argument 1",
               "positional argument 2",
               """--cluster-options={
                   'memory': '100G',
                   'nodes': '1',
                   'cpus': '20',
                   'partition': 'bigmem',
                   'job_name': 'JOBNAME',
                   'depends_on': '1,2,3,4,5,6',
                   'email_address': 'user@example.com',
                   'email_options': 'START,END,FAIL',
                   'time': '24:00:00',
                   'bash': '#!/bin/bash'}"""]
        expected_args = ["positional argument 1",
                         "positional argument 2"]
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        }
        }
        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1010(self):
        """
        positional arguments: 0
        keyword arguments: 1
        cluster_options_arguments: 0
        cluster_options_dictionary: 1
        """
        all = ["--keyword=keyword",
               "--longer-keyword-opt=meep",
               "--verbose",
               """--cluster-options={
                'memory': '100G',
                'nodes': '1',
                'cpus': '20',
                'partition': 'bigmem',
                'job_name': 'JOBNAME',
                'depends_on': '1,2,3,4,5,6',
                'email_address': 'user@example.com',
                'email_options': 'START,END,FAIL',
                'time': '24:00:00',
                'bash': '#!/bin/bash'}"""]
        expected_args = []
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        },
            'verbose': True,
            'longer_keyword_opt': 'meep',
            'keyword': 'keyword'
        }

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1011(self):
        """
        positional arguments: 1
        keyword arguments: 1
        cluster_options_arguments: 0
        cluster_options_dictionary: 1
        """
        all = ["positional 1",
               "positional 2",
               "--keyword=keyword",
               "--longer-keyword-opt=meep",
               "--verbose",
               """--cluster-options={
                'memory': '100G',
                'nodes': '1',
                'cpus': '20',
                'partition': 'bigmem',
                'job_name': 'JOBNAME',
                'depends_on': '1,2,3,4,5,6',
                'email_address': 'user@example.com',
                'email_options': 'START,END,FAIL',
                'time': '24:00:00',
                'bash': '#!/bin/bash'}"""]

        expected_args = ["positional 1", "positional 2"]
        expected_kwargs = {'cluster_options': {
            'memory': '100G',
            'nodes': '1',
            'cpus': '20',
            'partition': 'bigmem',
            'job_name': 'JOBNAME',
            'depends_on': '1,2,3,4,5,6',
            'email_address': 'user@example.com',
            'email_options': 'START,END,FAIL',
            'time': '24:00:00',
            'bash': '#!/bin/bash'
        },
            'verbose': True,
            'longer_keyword_opt': 'meep',
            'keyword': 'keyword'
        }

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1100(self):
        """
        positional arguments: 0
        keyword arguments: 0
        cluster_options_arguments: 1
        cluster_options_dictionary: 1
        """
        all = ["--memory=100G",
               "--nodes=1",
               "--cpus=30",
               "--partition=bigmem",
               "--job-name=JOBNAME",
               "--email-address=user@example.com",
               "--email-options=FAIL",
               "--time=0",
               """--cluster-options={
                   'memory': '100G',
                   'partition': 'bigmem',
                   'job_name': 'JOBNAME',
                   'depends_on': '1,2,3,4,5,6',
                   'email_address': 'user@example.com',
                   'email_options': 'START,END,FAIL',
                   'time': '24:00:00',
                   'bash': '#!/bin/bash',
                   'NEPA': 'CEQA',
                   'Jack': 'Black'}"""
               ]
        expected_args = []
        expected_kwargs = {'cluster_options': {'memory': '100G',
                                               'nodes': '1',
                                               'cpus': '30',
                                               'partition': 'bigmem',
                                               'job_name': 'JOBNAME',
                                               'email_address': 'user@example.com',
                                               'email_options': 'FAIL',
                                               'time': '0',
                                               'depends_on': '1,2,3,4,5,6',
                                               'bash': '#!/bin/bash'}}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1101(self):
        """
        positional arguments: 1
        keyword arguments: 0
        cluster_options_arguments: 1
        cluster_options_dictionary: 1
        """
        all = ["positional argument 1",
               "positional argument 2",
               "--memory=100G",
               "--nodes=1",
               "--cpus=30",
               "--partition=BIGMEM",
               "--job-name=JOBNAME",
               "--email-address=user@example.com",
               "--email-options=FAIL",
               "--time=0",
               """--cluster-options={
                   'memory': '100G',
                   'partition': 'bigmem',
                   'job_name': 'JOBNAME',
                   'depends_on': '1,2,3,4,5,6',
                   'email_address': 'user@example.com',
                   'email_options': 'START,END,FAIL',
                   'time': '24:00:00',
                   'bash': '#!/bin/bash',
                   'NEPA': 'CEQA',
                   'Jack': 'Black'}"""
               ]

        expected_args = ["positional argument 1", "positional argument 2"]
        expected_kwargs = {'cluster_options': {'memory': '100G',
                                               'nodes': '1',
                                               'cpus': '30',
                                               'partition': 'BIGMEM',
                                               'job_name': 'JOBNAME',
                                               'email_address': 'user@example.com',
                                               'email_options': 'FAIL',
                                               'time': '0',
                                               'depends_on': '1,2,3,4,5,6',
                                               'bash': '#!/bin/bash'}}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1110(self):
        """
        positional arguments: 0
        keyword arguments: 1
        cluster_options_arguments: 1
        cluster_options_dictionary: 1
        """
        all = ["--keyword1=keyword1",
               "--keyword2=keyword2",
               "--memory=100G",
               "--nodes=1",
               "--cpus=30",
               "--partition=BIGMEM",
               "--job-name=JOBNAME",
               "--email-address=user@example.com",
               "--email-options=FAIL",
               "--time=0",
               """--cluster-options={
                   'memory': '100G',
                   'partition': 'bigmem',
                   'job_name': 'JOBNAME',
                   'depends_on': '1,2,3,4,5,6',
                   'email_address': 'user@example.com',
                   'email_options': 'START,END,FAIL',
                   'time': '24:00:00',
                   'bash': '#!/bin/bash',
                   'NEPA': 'CEQA',
                   'Jack': 'Black'}"""
               ]

        expected_args = []
        expected_kwargs = {'cluster_options': {'memory': '100G',
                                               'nodes': '1',
                                               'cpus': '30',
                                               'partition': 'BIGMEM',
                                               'job_name': 'JOBNAME',
                                               'email_address': 'user@example.com',
                                               'email_options': 'FAIL',
                                               'time': '0',
                                               'depends_on': '1,2,3,4,5,6',
                                               'bash': '#!/bin/bash'},
                           'keyword1': 'keyword1',
                           'keyword2': 'keyword2'}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_1111(self):
        """
        positional arguments: 1
        keyword arguments: 1
        cluster_options_arguments: 1
        cluster_options_dictionary: 1
        """
        all = ["positional argument 1",
               "positional argument 2",
               "--keyword1=keyword1",
               "--keyword2=keyword2",
               "--memory=100G",
               "--nodes=1",
               "--cpus=30",
               "--partition=BIGMEM",
               "--job-name=JOBNAME",
               "--email-address=user@example.com",
               "--email-options=FAIL",
               "--time=0",
               """--cluster-options={
                   'memory': '100G',
                   'partition': 'bigmem',
                   'job_name': 'JOBNAME',
                   'depends_on': '1,2,3,4,5,6',
                   'email_address': 'user@example.com',
                   'email_options': 'START,END,FAIL',
                   'time': '24:00:00',
                   'bash': '#!/bin/bash',
                   'NEPA': 'CEQA',
                   'Jack': 'Black'}"""
               ]

        expected_args = ["positional argument 1", "positional argument 2"]
        expected_kwargs = {'cluster_options': {'memory': '100G',
                                               'nodes': '1',
                                               'cpus': '30',
                                               'partition': 'BIGMEM',
                                               'job_name': 'JOBNAME',
                                               'email_address': 'user@example.com',
                                               'email_options': 'FAIL',
                                               'time': '0',
                                               'depends_on': '1,2,3,4,5,6',
                                               'bash': '#!/bin/bash'},
                           'keyword1': 'keyword1',
                           'keyword2': 'keyword2'}

        (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
        self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_broken1(self):
        """
        Broken Arguments
        """
        all = [3, '--key1=4', 5, '--key2=5']
        expected_args = [3, 5]
        expected_kwargs = {'key1': 4,
                           'key2': 5}
        with self.assertRaises(AttributeError):
            (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
            self.evaluate(args, expected_args, kwargs, expected_kwargs)

    def test_broken2(self):
        """
        Broken Arguments
        """
        all = ['--=--', '--x + 7=5 == 5']
        expected_args = []
        expected_kwargs = {'x + 7': '5 == 5', '--': '--'}

        with self.assertRaises(AssertionError):
            (args, kwargs) = run_parallel_command_with_args(self.test_func, all)
            self.evaluate(args, expected_args, kwargs, expected_kwargs)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestArgParse)
    unittest.TextTestRunner(verbosity=3).run(suite)
