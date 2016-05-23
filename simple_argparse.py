"""
From Perlence/simple_argparse.py
"""
import sys
from ast import literal_eval


def run_parallel_command_with_args(func, args=None):
    """
    Parse arguments from argv or given list of args, parse them for usage with
    the ParallelCommand / PairedEndCommand class,
    """
    (args, kwargs) = parse_args(args)


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
        args = sys.argv[1:]

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
