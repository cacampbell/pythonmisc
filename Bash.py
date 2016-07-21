#!/usr/bin/env python
import errno
import sys
from shlex import split
from subprocess import CalledProcessError
from subprocess import PIPE
from subprocess import Popen

import os


def mkdir_p(p):
    """
    Emulates UNIX `mkdir -p` functionality
    Attempts to make a directory, if it fails, error unless the failure was
    due to the directory already existing
    :param: p: str: the path to make
    :return:
    :raises: OSError: If not "directory already exists"
    """
    try:
        os.makedirs(p)
    except OSError as err:
        if err.errno == errno.EEXIST and os.path.isdir(p):
            print("{} already exists".format(p),
                  file=sys.stderr)
        else:
            raise (err)


def bash(command, *args):
    """
    Dispatches the command / script <command> to the bash shell with the
    arguments that might be listed in args
    :param command: str: a command / script to be sent to bash shell
    :param args: str | list<str>: string of command args or list of strings
    :return: tuple<str,str>: the decoded (output, error) of the command, script
    :raises: OSError: running the command failed
    :raises: CalledProcessError: the called command failed
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
        # Same is true for permission denied errors
        if err.errno == errno.ENOENT or err.errno == errno.EACCES:
            try:
                process = Popen(cmd_args, stdin=PIPE, stdout=PIPE, stderr=PIPE,
                                shell=True)  # this time with subshell
                return __decode(process.communicate())
            except Exception as err2:
                raise (err2)  # rerunning with shell also failed
        else:
            raise (err)  # Could not successfully execute the command
    except CalledProcessError as err:
        print("Error while calling command: {}\n{}".format(command, err))
        raise (err)


# The following is taken from shutil.which / whichcraft
# https://github.com/pydanny/whichcraft/blob/master/whichcraft.py#L20
def which(cmd, mode=os.F_OK | os.X_OK, path=None):
    """Given a command, mode, and a PATH string, return the path which
    conforms to the given mode on the PATH, or None if there is no such
    file.
    `mode` defaults to os.F_OK | os.X_OK. `path` defaults to the result
    of os.environ.get("PATH"), or can be overridden with a custom search
    path.
    Note: This function was backported from the Python 3 source code.
    """

    # Check that a given file can be accessed with the correct mode.
    # Additionally check that `file` is not a directory, as on Windows
    # directories pass the os.access check.
    def _access_check(fn, mode):
        return (os.path.exists(fn) and os.access(fn, mode) and
                not os.path.isdir(fn))

    # If we're given a path with a directory part, look it up directly
    # rather than referring to PATH directories. This includes checking
    # relative to the current directory, e.g. ./script
    if os.path.dirname(cmd):
        if _access_check(cmd, mode):
            return cmd
        return None

    if path is None:
        path = os.environ.get("PATH", os.defpath)
    if not path:
        return None
    path = path.split(os.pathsep)

    if sys.platform == "win32":
        # The current directory takes precedence on Windows.
        if os.curdir not in path:
            path.insert(0, os.curdir)

        # PATHEXT is necessary to check on Windows.
        pathext = os.environ.get("PATHEXT", "").split(os.pathsep)
        # See if the given file matches any of the expected path
        # extensions. This will allow us to short circuit when given
        # "python.exe". If it does match, only test that one, otherwise we
        # have to try others.
        if any(cmd.lower().endswith(ext.lower()) for ext in pathext):
            files = [cmd]
        else:
            files = [cmd + ext for ext in pathext]
    else:
        # On other platforms you don't have things like PATHEXT to tell you
        # what file suffixes are executable, so just pass on cmd as-is.
        files = [cmd]

    seen = set()
    for dir in path:
        normdir = os.path.normcase(dir)
        if normdir not in seen:
            seen.add(normdir)
            for thefile in files:
                name = os.path.join(dir, thefile)
                if _access_check(name, mode):
                    return name
    return None
