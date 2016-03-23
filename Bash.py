#!/usr/bin/env python
import errno
from subprocess import CalledProcessError
from subprocess import PIPE
from subprocess import Popen
from shlex import split


def bash(command, *args):
    """
    Dispatches the command / script <command> to the bash shell with the
    arguments that might be listed in args
    :param command: str: a command / script to be sent to bash shell
    :param args: str | list<str>: string of command args or list of strings
    :return: tuple<str,str>: the decoded (output, error) of the command, script
    """

    def __decode(out_err):
        return (out_err[0].decode("utf-8"), out_err[1].decode("utf-8"))

    cmd_args = [command]

    if len(args) == 1:  # A string containing command line arguments
        cmd_args.extend(split(args[0]))
    else:  # actually a list of command line arguments
        for arg in args:
            cmd_args.extend(split(arg))  # split each item into args

    try:  # try dispatching the command without a subshell
        process = Popen(cmd_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        return __decode(process.communicate())
    except OSError as err:  # OS Error
        #  No such file or directory error, which can occur when the command
        #  neeeds a subshell to work correctly. So, in this case, recall the
        #  command / script with a subshell
        if err.errno == errno.ENOENT:
            process = Popen(cmd_args, stdin=PIPE, stdout=PIPE, stderr=PIPE,
                            shell=True)  # this time with subshell
            return __decode(process.communicate())
        else:
            raise (err)  # :(
    except CalledProcessError as err:
        print("Error while calling command: {}\n{}".format(command, err))
        raise (err)