"""
From Perlence/simple_argparse.py
"""
import unittest
from ast import literal_eval
from sys import argv
from sys import stderr


def run_parallel_command_with_args(func, args=None):
    """
    Parse arguments from argv or given list of args, parse them for usage with
    the ParallelCommand / PairedEndCommand class,
    """
    (args, kwargs) = parse_args(args)
    cluster_options_keys = ("memory",
                            "nodes",
                            "cpus",
                            "partition",
                            "job_name",
                            "depends_on",
                            "email_user",
                            "email_options",
                            "time",
                            "bash")

    cluster_options_found = dict((k, kwargs[k]) for k in cluster_options_keys)
    new_kwargs = dict()
    cluster_options = dict()

    if "cluster_options" in kwargs.keys():
        try:
            assert (type(kwargs["cluster_options"]) is type(dict()))
            user_cluster_options = kwargs["cluster_options"]
            for key in user_cluster_options.keys():
                if key in cluster_options_keys:
                    cluster_options[key] = user_cluster_options[key]

        except AssertionError:
            print("Supplied non-dictionary to cluster_options, ignoring...",
                  file=stderr)


    for key, val in kwargs.items():
        if key not in cluster_options_keys:
            new_kwargs[key] = kwargs[key]

    new_kwargs["cluster_options"] = cluster_options
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

    def test_run_with_args(self):
        run_with_args(test_func,
                      "positional argument 1",
                      "positional argument 2",
                      "--cpus=43",
                      "--email-address=example@example.com",
                      '--cluster_options=dict(memory="5G", mail_options="END")',
                      "--memory=50G")

    def test_run_parallel_command_with_args(self):
        run_parallel_command_with_args(
            test_func,
            "positional argument 1",
            "positional argument 2",
            "--cpus=43",
            "--email-address=example@example.com",
            '--cluster_options=dict(memory="5G", mail_options="END")',
            "--memory=50G")


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestArgParse)
    unittest.TextTestRunner(verbosity=3).run(suite)
