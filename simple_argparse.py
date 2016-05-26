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
                   "email_user",
                   "email_options",
                   "time",
                   "bash")


def run_parallel_command_with_args(func, args=None):
    """
    Parse arguments from argv or given list of args, parse them for usage with
    the ParallelCommand / PairedEndCommand class,
    """
    (args, kwargs) = parse_args(args)
    new_kwargs = dict()
    cluster_options = {}

    if "cluster_options" in kwargs.keys():
        cluster_options = kwargs["cluster_options"]

    for (key, value) in kwargs.items():
        if key in CLUSTER_OPTIONS:
            cluster_options[key] = str(value)

    cluster_options_copy = cluster_options.copy()
    for (key, value) in cluster_options_copy.items():
        if key not in CLUSTER_OPTIONS:
            cluster_options.pop(key)
            print("Removing Unexpected cluster option: {}".format(key),
                  file=stderr)

    if cluster_options != {}:
        new_kwargs["cluster_options"] = cluster_options

    for key in kwargs.keys():
        if key not in CLUSTER_OPTIONS:
            new_kwargs[key] = str(kwargs[key])

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

            positional = list(args)
            keywords = dict(kwargs)
            return (positional, keywords)

        self.test_func = test_func

    def test_1(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--memory=40G",
                     "--nodes=1",
                     "--cpus=10",
                     "--partition=bigmem",
                     "--job-name=test_job",
                     "--depends-on=12345,12346,12347",
                     "--email-user=test@example.com",
                     "--email-options=END,FAIL,START",
                     "--time=24:00:00",
                     "--bash='#!/usr/bin/env bash'",
                     "--verbose",
                     "--dry-run",
                     "--output-root=/home/example/output",
                     "--input-root=/home/example/input",
                     "--stats=True",
                     "--test-keyword='Test Keyword Value'"]
        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)
        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")
        expected_dict = {'memory': '40G',
                         'nodes': '1',
                         'cpus': '10',
                         'partition': 'bigmem',
                         'job_name': 'test_job',
                         'depends_on': '12345,12346,12347',
                         'email_user': 'test@example.com',
                         'email_options': 'END,FAIL,START',
                         'time': '24:00:00',
                         'bash': '#!/usr/bin/env bash'}

        for (key, value) in dict(kwargs["cluster_options"]).items():
            try:
                assert (expected_dict[key] == value)
            except AssertionError as err:
                print("Disagreement: {}".format(key))
                raise (err)

        assert (kwargs["cluster_options"] == expected_dict)
        assert (kwargs["verbose"] is True)
        assert (kwargs["dry_run"] is True)
        assert (kwargs["input_root"] == "/home/example/input")
        assert (kwargs["output_root"] == "/home/example/output")
        assert (kwargs["stats"] is True)
        assert (kwargs["test_keyword"] == "Test Keyword Value")

    def test_2(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--memory=40G",
                     "--nodes=1",
                     "--cpus=10",
                     "--partition=bigmem",
                     "--job-name=test_job",
                     "--depends-on=12345,12346,12347",
                     "--email-user=test@example.com",
                     "--email-options=END,FAIL,START",
                     "--time=24:00:00",
                     "--bash='#!/usr/bin/env bash'",
                     "--verbose",
                     "--dry-run",
                     "--output-root=/home/example/output",
                     "--input-root=/home/example/input",
                     "--stats=True",
                     "--test-keyword='Test Keyword Value'",
                     "--cluster-options={}"]

        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)

        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")
        expected_dict = {'memory': '40G',
                         'nodes': '1',
                         'cpus': '10',
                         'partition': 'bigmem',
                         'job_name': 'test_job',
                         'depends_on': '12345,12346,12347',
                         'email_user': 'test@example.com',
                         'email_options': 'END,FAIL,START',
                         'time': '24:00:00',
                         'bash': '#!/usr/bin/env bash'}

        for (key, value) in dict(kwargs["cluster_options"]).items():
            try:
                assert (expected_dict[key] == value)
            except AssertionError as err:
                print("Disagreement: {}".format(key))
                raise (err)

        assert (kwargs["cluster_options"] == expected_dict)

        assert (kwargs["verbose"] is True)
        assert (kwargs["dry_run"] is True)
        assert (kwargs["input_root"] == "/home/example/input")
        assert (kwargs["output_root"] == "/home/example/output")
        assert (kwargs["stats"] is True)
        assert (kwargs["test_keyword"] == "Test Keyword Value")

    def test_3(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--memory=40G",
                     "--nodes=1",
                     "--cpus=10",
                     "--partition=bigmem",
                     "--job-name=test_job",
                     "--depends-on=12345,12346,12347",
                     "--email-user=test@example.com",
                     "--email-options=END,FAIL,START",
                     "--time=24:00:00",
                     "--bash='#!/usr/bin/env bash'",
                     "--verbose",
                     "--dry-run",
                     "--output-root=/home/example/output",
                     "--input-root=/home/example/input",
                     "--stats=True",
                     "--test-keyword='Test Keyword Value'",
                     "--cluster-options={'memory':'4000000G', "
                     "'cpus':'2000', 'nodes':'50', 'partition':'shortq',"
                     " 'job_name':'test_job_name', 'depends_on':'123,456,123',"
                     " 'email_user':'a@b.c.net', 'email_options':'START,FAIL',"
                     " 'time':'0'}"]
        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)

        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")

        expected_dict = {'memory': '40G',
                         'nodes': '1',
                         'cpus': '10',
                         'partition': 'bigmem',
                         'job_name': 'test_job',
                         'depends_on': '12345,12346,12347',
                         'email_user': 'test@example.com',
                         'email_options': 'END,FAIL,START',
                         'time': '24:00:00',
                         'bash': '#!/usr/bin/env bash'}

        for (key, value) in dict(kwargs["cluster_options"]).items():
            try:
                assert (expected_dict[key] == value)
            except AssertionError as err:
                print("Disagreement: {}".format(key))
                raise (err)

        assert (kwargs["cluster_options"] == expected_dict)
        assert (kwargs["verbose"] is True)
        assert (kwargs["dry_run"] is True)
        assert (kwargs["input_root"] == "/home/example/input")
        assert (kwargs["output_root"] == "/home/example/output")
        assert (kwargs["stats"] is True)
        assert (kwargs["test_keyword"] == "Test Keyword Value")

    def test_4(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--cluster-options={'memory':'4000000G', "
                     "'cpus':'2000', 'nodes':'50', 'partition':'shortq',"
                     " 'job_name':'test_job_name', 'depends_on':'123,456,123',"
                     " 'email_user':'a@b.c.net', 'email_options':'START,FAIL',"
                     " 'time':'0', 'bash':'#!/bin/bash'}"]

        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)

        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")
        expected_dict = {'memory': '4000000G',
                         'nodes': '50',
                         'cpus': '2000',
                         'partition': 'shortq',
                         'job_name': 'test_job_name',
                         'depends_on': '123,456,123',
                         'email_user': 'a@b.c.net',
                         'email_options': 'START,FAIL',
                         'time': '0',
                         'bash': '#!/bin/bash'}

        for (key, value) in dict(kwargs["cluster_options"]).items():
            try:
                assert (expected_dict[key] == value)
            except AssertionError as err:
                print("Disagreement: {}".format(key))
                raise (err)

        assert (kwargs["cluster_options"] == expected_dict)

    def test_5(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--memory=40G",
                     "--nodes=1",
                     "--cpus=10",
                     "--bash='#!/usr/bin/env bash'",
                     "--verbose",
                     "--dry-run",
                     "--output-root=/home/example/output",
                     "--input-root=/home/example/input",
                     "--stats=True",
                     "--test-keyword='Test Keyword Value'",
                     "--cluster-options={'memory':'4000000G', "
                     "'fake':'fake', 'fake2':'fake', 'partition':'shortq',"
                     " 'job_name':'test_job_name', 'depends_on':'123,456,123',"
                     " 'email_user':'a@b.c.net', 'email_options':'START,FAIL',"
                     " 'time':'0', 'bash':'#!/bin/bash'}"]
        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)

        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")
        expected_dict = {'memory': '40G',
                         'nodes': '1',
                         'cpus': '10',
                         'partition': 'shortq',
                         'job_name': 'test_job_name',
                         'depends_on': '123,456,123',
                         'email_user': 'a@b.c.net',
                         'email_options': 'START,FAIL',
                         'time': '0',
                         'bash': '#!/usr/bin/env bash'}

        for (key, value) in dict(kwargs["cluster_options"]).items():
            try:
                assert (expected_dict[key] == value)
            except AssertionError as err:
                print("Disagreement: {}".format(key))
                raise (err)

        assert (kwargs["cluster_options"] == expected_dict)
        assert (kwargs["verbose"] is True)
        assert (kwargs["dry_run"] is True)
        assert (kwargs["input_root"] == "/home/example/input")
        assert (kwargs["output_root"] == "/home/example/output")
        assert (kwargs["stats"] is True)
        assert (kwargs["test_keyword"] == "Test Keyword Value")

    def test_6(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--memory=40G",
                     "--nodes=1",
                     "--cpus=10",
                     "--partition=bigmem",
                     "--job-name=test_job",
                     "--depends-on=12345,12346,12347",
                     "--email-user=test@example.com",
                     "--email-options=END,FAIL,START",
                     "--time=24:00:00",
                     "--bash='#!/usr/bin/env bash'",
                     "--verbose",
                     "--dry-run",
                     "--output-root=/home/example/output",
                     "--input-root=/home/example/input",
                     "--stats=True",
                     "--test-keyword='Test Keyword Value'",
                     "--cluster-options={'memory':'4000000G', "
                     "'cpus':'2000', 'nodes':'50', 'partition':'shortq',"
                     " 'job_name':'test_job_name', 'depends_on':'123,456,123',"
                     " 'email_user':'a@b.c.net', 'email_options':'START,FAIL',"
                     " 'time':'0', 'bash':'#!/bin/bash'}"]
        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)

        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")
        expected_dict = {'memory': '40G',
                         'nodes': '1',
                         'cpus': '10',
                         'partition': 'bigmem',
                         'job_name': 'test_job',
                         'depends_on': '12345,12346,12347',
                         'email_user': 'test@example.com',
                         'email_options': 'END,FAIL,START',
                         'time': '24:00:00',
                         'bash': '#!/usr/bin/env bash'}
        for (key, value) in dict(kwargs["cluster_options"]).items():
            try:
                assert (expected_dict[key] == value)
            except AssertionError as err:
                print("Disagreement: {}".format(key))
                raise (err)

        assert (kwargs["cluster_options"] == expected_dict)
        assert (kwargs["verbose"] is True)
        assert (kwargs["dry_run"] is True)
        assert (kwargs["input_root"] == "/home/example/input")
        assert (kwargs["output_root"] == "/home/example/output")
        assert (kwargs["stats"] is True)
        assert (kwargs["test_keyword"] == "Test Keyword Value")

    def test_7(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--verbose",
                     "--dry-run",
                     "--output-root=/home/example/output",
                     "--input-root=/home/example/input",
                     "--stats=True",
                     "--test-keyword='Test Keyword Value'",
                     "--cluster-options={'memory':'4000000G', "
                     "'cpus':'2000', 'nodes':'50', 'partition':'shortq',"
                     " 'job_name':'test_job_name', 'depends_on':'123,456,123',"
                     " 'email_user':'a@b.c.net', 'email_options':'START,FAIL',"
                     " 'time':'0', 'bash':'#!/bin/bash'}"]
        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)

        expected_dict = {'memory': '4000000G',
                         'nodes': '50',
                         'cpus': '2000',
                         'partition': 'shortq',
                         'job_name': 'test_job_name',
                         'depends_on': '123,456,123',
                         'email_user': 'a@b.c.net',
                         'email_options': 'START,FAIL',
                         'time': '0',
                         'bash': '#!/bin/bash'}
        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")

        for (key, value) in dict(kwargs["cluster_options"]).items():
            try:
                assert (expected_dict[key] == value)
            except AssertionError as err:
                print("Disagreement: {}".format(key))
                raise (err)

        assert (kwargs["cluster_options"] == expected_dict)
        assert (kwargs["verbose"] is True)
        assert (kwargs["dry_run"] is True)
        assert (kwargs["input_root"] == "/home/example/input")
        assert (kwargs["output_root"] == "/home/example/output")
        assert (kwargs["stats"] is True)
        assert (kwargs["test_keyword"] == "Test Keyword Value")

    def test_8(self):
        arguments = ["positional argument 1",
                     "positional argument 2",
                     "--verbose",
                     "--dry-run",
                     "--output-root=/home/example/output",
                     "--input-root=/home/example/input",
                     "--stats=True",
                     "--test-keyword='Test Keyword Value'",
                     ]
        (args, kwargs) = run_parallel_command_with_args(self.test_func,
                                                        arguments)
        assert (args[0] == "positional argument 1")
        assert (args[1] == "positional argument 2")
        assert (kwargs["verbose"] is True)
        assert (kwargs["dry_run"] is True)
        assert (kwargs["input_root"] == "/home/example/input")
        assert (kwargs["output_root"] == "/home/example/output")
        assert (kwargs["stats"] is True)
        assert (kwargs["test_keyword"] == "Test Keyword Value")

        with self.assertRaises(KeyError):
            print(kwargs["cluster_options"], file=stderr)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestArgParse)
    unittest.TextTestRunner(verbosity=3).run(suite)
