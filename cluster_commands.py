#!/usr/bin/env python3
from pwd import getpwuid
from xml.etree.ElementTree import XML
from xml.etree.ElementTree import XMLParser

from os import environ
from os import getuid

from Bash import bash
from Bash import which


def get_username():
    return getpwuid(getuid())[0]


def __get_backend():
    print(which("scontrol"))
    if which("scontrol"):
        return ("slurm")
    elif which("qstat"):
        return ("torque")
    else:
        raise(RuntimeError("No suitable cluster backend found."))


# Simple way to determine the backend for the cluster -- using API for commands
__BACKEND__ = None

if "CLUSTER_BACKGROUND" in environ:
    __BACKEND__ = environ["CLUSTER_BACKEND"]
else:
    __BACKEND__ = __get_backend()


def __slurm_e_opts(str):
    options = ""

    for opt in str.split(","):
        opt = opt.upper().strip()
        if opt == "BEGIN" or opt == "START":
            options += "START,"
        if opt == "END" or opt == "FINISH":
            options += "END,"
        if opt == "FAIL" or opt == "ABORT":
            options += "FAIL,"

    return (options.rstrip(","))


def __submit_slurm(**kwargs):
    """
    Anticipated Keyword Arguments:
        memory - The memory to be allocated to this job
        nodes - The nodes to be allocated
        cpus - The cpus **per node** to request
        partition -  The queue name or partition name for the submitted job
        job_name - The name of the job
        depends_on - The dependencies (as comma separated list of job numbers)
        email_address -  The email address to use for notifications
        email_options - Email options: START|BEGIN,END|FINISH,FAIL|ABORT
        time - time to request from the scheduler
        bash -  The bash shebang line to use in the script
        input - The input filename for the job
        output - The output filename for the job
        error - The error filename for the job
    """
    submit_cmd = ("sbatch")

    if "memory" in kwargs.keys():
        submit_cmd += (" --mem={}").format(kwargs["memory"])
    if "nodes" in kwargs.keys():
        submit_cmd += (" --ntasks={}").format(kwargs["nodes"])
    if "cpus" in kwargs.keys():
        submit_cmd += (" --cpus-per-task={}").format(kwargs["cpus"])
    if "partition" in kwargs.keys():
        submit_cmd += (" --partition={}").format(kwargs["partition"])
    if "job_name" in kwargs.keys():
        submit_cmd += (" --job-name={}").format(kwargs["job_name"])
    if "depends_on" in kwargs.keys():
        submit_cmd += (" --dependency=after_ok:{}").format(kwargs["depends_on"])
    if "email_address" in kwargs.keys():
        submit_cmd += (" --mail-user={}").format(kwargs["email_address"])
    if "email_options" in kwargs.keys():
        submit_cmd += (" --mail-type={}").format(
            __slurm_e_opts(kwargs["email_options"]))
    if "input" in kwargs.keys():
        submit_cmd += (" --input={}").format(kwargs["input"])
    if "output" in kwargs.keys():
        submit_cmd += (" --output={}").format(kwargs["output"])
    if "error" in kwargs.keys():
        submit_cmd += (" --error={}").format(kwargs["error"])

    return (submit_cmd)


def __torque_e_opts(str):
    options = ""

    for opt in str.split(","):
        opt = opt.upper().strip()
        if opt == "BEGIN" or opt == "START":
            options += "b"
        if opt == "END":
            options += "e"
        if opt == "FAIL" or opt == "ABORT":
            options += "a"

    return (options)


def __submit_torque(**kwargs):
    """
    Anticipated Keyword Arguments:
        memory - The memory to be allocated to this job
        nodes - The nodes to be allocated
        cpus - The cpus **per node** to request
        partition -  The queue name or partition name for the submitted job
        job_name - The name of the job
        depends_on - The dependencies (as comma separated list of job numbers)
        email_address -  The email address to use for notifications
        email_options - Email options: START|BEGIN,END|FINISH,FAIL|ABORT
        time - time to request from the scheduler
        bash -  The bash shebang line to use in the script
        input - The input filename for the job
        output - The output filename for the job
        error - The error filename for the job
    """

    submit_cmd = ("qsub")
    if "memory" in kwargs.keys() and "cpus" in kwargs.keys() \
            and "nodes" in kwargs.keys():
        submit_cmd += " -l mem={m},nodes={n}:ppn={c}".format(
            m=kwargs["memory"], n=kwargs["nodes"], c=kwargs["cpus"]
        )
    elif "memory" in kwargs.keys() and "cpus" in kwargs.keys():
        submit_cmd += " -l mem={m},nodes=1:ppn={c}".format(
            m=kwargs["memory"], c=kwargs["cpus"]
        )
    elif "memory" in kwargs.keys() and "nodes" in kwargs.keys():
        submit_cmd += " -l mem={m},nodes={n}".format(
            m=kwargs["memory"], n=kwargs["nodes"]
        )
    elif "cpus" in kwargs.keys() and "nodes" in kwargs.keys():
        submit_cmd += " -l nodes={n}:ppn={c}".format(
            n=kwargs["nodes"], c=kwargs["cpus"]
        )
    else:
        if "memory" in kwargs.keys():
            submit_cmd += " -l {}".format(kwargs["memory"])
        if "cpus" in kwargs.keys():
            submit_cmd += " -l nodes=1:ppn={}".format(kwargs["cpus"])
        if "nodes" in kwargs.keys():
            submit_cmd += " -l nodes={}".format(kwargs["nodes"])

    if "partition" in kwargs.keys():
        submit_cmd += " -q {}".format(kwargs["partition"])
    if "job_name" in kwargs.keys():
        submit_cmd += " -N {}".format(kwargs["job_name"])
    if "depends_on" in kwargs.keys():
        submit_cmd += " -hold_jid {}".format(kwargs["depends_on"])
    if "email_address" in kwargs.keys():
        submit_cmd += " -M {}".format(kwargs["email_address"])
    if "email_options" in kwargs.keys():
        submit_cmd += " -m {}".format(__torque_e_opts(kwargs["email_options"]))
    if "input" in kwargs.keys():
        submit_cmd += " -i {}".format(kwargs["input"])
    if "output" in kwargs.keys():
        submit_cmd += " -o {}".format(kwargs["output"])
    if "error" in kwargs.keys():
        submit_cmd += " -e {}".format(kwargs["error"])

    return (submit_cmd)


def submit_job(command_str, **kwargs):
    """
    Anticipated positional args:
        command_str - The command to be wrapped for submission to scheduler

    Anticipated keyword args:
        memory - The memory to be allocated to this job
        nodes - The nodes to be allocated
        cpus - The cpus **per node** to request
        partition -  The queue name or partition name for the submitted job
        job_name - The name of the job
        depends_on - The dependencies (as comma separated list of job numbers)
        email_address -  The email address to use for notifications
        email_options - Email options: START|BEGIN,END|FINISH,FAIL|ABORT
        time - time to request from the scheduler
        bash -  The bash shebang line to use in the script
        input - The input filename for the job
        output - The output filename for the job
        error - The error filename for the job
    """
    shebang_line = "#!/usr/bin/env bash"

    if "bash" in kwargs:
        shebang_line = kwargs["bash"]

    script = ("{shebang_line}\n{command}").format(shebang_line=shebang_line,
                                                  command=command_str)
    sub_command = ("echo '{}' | {}")
    sub_script = ""  # Will hold entire string that will be send to bash shell

    if __BACKEND__ == "slurm":  # Format with slurm options
        sub_script = sub_command.format(script, __submit_slurm(**kwargs))
    elif __BACKEND__ == "torque":  # Format with torque options
        sub_script = sub_command.format(script, __submit_torque(**kwargs))

    (stdout, stderr) = bash(sub_script)  # Actaully call the script using bash

    try:  # To parse the output based on expected successful submission result
        if __BACKEND__ == "slurm":
            # Successfully submitted job <Job ID>
            return (stdout.split(" ")[-1].strip("\n"))
        if __BACKEND__ == "torque":
            # <Job ID>.hostname.etc.etc
            return (stdout.split(".")[0])

    except (ValueError, IndexError) as err:
        print("Could not capture Job ID! Dependency checks may fail!")
        print("Err: {}".format(err))
        return ("")


def __cancel_jobs_slurm(*args):
    pass


def __cancel_jobs_torque(*args):
    pass


def cancel_jobs(*args):
    if __BACKEND__ == "slurm":
        __cancel_jobs_slurm(*args)
    elif __BACKEND__ == "torque":
        __cancel_jobs_torque(*args)


def __cancel_suspended_jobs_slurm():
    pass


def __cancel_suspended_jobs_torque():
    pass


def cancel_suspended_jobs():
    if __BACKEND__ == "slurm":
        __cancel_suspended_jobs_slurm()
    elif __BACKEND__ == "torque":
        __cancel_suspended_jobs_torque()


def __requeue_suspended_jobs_slurm():
    pass


def __requeue_suspended_jobs_torque():
    pass


def requeue_suspended_jobs():
    if __BACKEND__ == "slurm":
        __requeue_suspended_jobs_slurm()
    elif __BACKEND__ == "torque":
        __requeue_suspended_jobs_torque()


def __existing_jobs_slurm():
    (out, err) = bash("squeue --noheader -o %j -u {}".format(get_username()))
    return (out.splitlines())


def __existing_jobs_torque():
    (out, err) = bash("qstat -xml -u {}".format(get_username()))
    xml = XML(out, parser=XMLParser(encoding='utf-8'))
    return ([node.text for node in xml.iter("JB_name")])


def existing_jobs():
    if __BACKEND__ == "slurm":
        return __existing_jobs_slurm()
    elif __BACKEND__ == "torque":
        return __existing_jobs_torque()
