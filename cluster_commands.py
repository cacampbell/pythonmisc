#!/usr/bin/env python3
from pwd import getpwuid

from os import environ
from os import getuid

from Bash import bash
from Bash import which
from slurm_commands import scancel
from slurm_commands import scontrol
from slurm_commands import squeue
from torque_commands import qdel, qstat

def get_username():
    return getpwuid(getuid())[0]


def __check(key, dictionary):
    try:
        assert (isinstance(dictionary, dict))
        assert (isinstance(key, str))
    except AssertionError as err:
        print("Could not check dictionary for key: {}".format(err))
        raise (err)

    if key in dictionary.keys():
        if dictionary[key] is not None and dictionary[key] is not "":
            return True
    return False


def get_backend():
    if "CLUSTER_BACKEND" in environ:
        return environ["CLUSTER_BACKEND"]

    if which("scontrol") is not None:
        environ["CLUSTER_BACKEND"] = "slurm"
        return ("slurm")
    elif which("qstat") is not None:
        environ["CLUSTER_BACKEND"] = "torque"
        return ("torque")
    else:
        raise(RuntimeError("No suitable cluster backend found."))


def __slurm_e_opts(string):
    # Possible SLURM email options include: NONE, BEGIN, END, FAIL, REQUEUE,
    # STAGE_OUT, ALL (equivalent to BEGIN, END, FAIL, REQUEUE, STAGE_OUT)
    # This interface will handle only those options also supported by Torque:
    # BEGIN, END, FAIL (ALL will be equivalent to BEGIN,END,FAIL)
    options = ""

    if "," in string.strip():
        for chunk in string.split(","):
            new_chunk = chunk.upper().strip()

            if new_chunk == "END" or new_chunk == "FAIL" or \
                            new_chunk == "BEGIN" or new_chunk == "ALL" or \
                            new_chunk == "ABORT":
                    options = options + new_chunk + ","

    else:
        options = string

    return(options.rstrip(","))



def __slurm_dep(jobs_obj):
    job_str = ""
    # Either Tuple<int> or List<str> by parsing cmdline with commas
    if type(jobs_obj) is tuple or type(jobs_obj) is list:
        for job in jobs_obj:
            job_str += "{}:".format(str(job))

        return(job_str.rstrip(":"))
    # Or, parsed as a str or int
    if type(jobs_obj) is str or type(jobs_obj) is int:
        return str(jobs_obj)


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

    if __check("memory", kwargs):
        submit_cmd += (" --mem={}").format(kwargs["memory"])
    if __check("nodes", kwargs):
        submit_cmd += (" --ntasks={}").format(kwargs["nodes"])
    if __check("cpus", kwargs):
        submit_cmd += (" --cpus-per-task={}").format(kwargs["cpus"])
    if __check("partition", kwargs):
        submit_cmd += (" --partition={}").format(kwargs["partition"])
    if __check("job_name", kwargs):
        submit_cmd += (" --job-name={}").format(kwargs["job_name"])
    if __check("depends_on", kwargs):
        submit_cmd += (" --dependency=after_ok:{}").format(__slurm_dep(
            kwargs["depends_on"]))
    if __check("email_address", kwargs):
        submit_cmd += (" --mail-user={}").format(kwargs["email_address"])
    if __check("email_options", kwargs):
        submit_cmd += (" --mail-type={}").format(
            __slurm_e_opts(kwargs["email_options"]))
    if __check("input", kwargs):
        submit_cmd += (" --input={}").format(kwargs["input"])
    if __check("output", kwargs):
        submit_cmd += (" --output={}").format(kwargs["output"])
    if __check("error", kwargs):
        submit_cmd += (" --error={}").format(kwargs["error"])

    return (submit_cmd)


def __torque_e_opts(string):
    options = ""
    if "," in string:
        for option in string.split(","):
            opt = option.strip().upper()
            if opt == "BEGIN" or opt == "START":
                options = options + "b"
            if opt == "END":
                options = options + "e"
            if opt == "FAIL" or opt == "ABORT":
                options = options + "a"

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
    if __check("memory", kwargs) and __check("cpus", kwargs) and __check(
            "nodes", kwargs):
        submit_cmd += " -l mem={m},nodes={n}:ppn={c}".format(
            m=kwargs["memory"], n=kwargs["nodes"], c=kwargs["cpus"]
        )
    elif __check("memory", kwargs) and __check("cpus", kwargs):
        submit_cmd += " -l mem={m},nodes=1:ppn={c}".format(
            m=kwargs["memory"], c=kwargs["cpus"]
        )
    elif __check("memory", kwargs) and __check("nodes", kwargs):
        submit_cmd += " -l mem={m},nodes={n}".format(
            m=kwargs["memory"], n=kwargs["nodes"]
        )
    elif __check("cpus", kwargs) and __check("nodes", kwargs):
        submit_cmd += " -l nodes={n}:ppn={c}".format(
            n=kwargs["nodes"], c=kwargs["cpus"]
        )
    else:
        if __check("memory", kwargs):
            submit_cmd += " -l {}".format(kwargs["memory"])
        if __check("cpus", kwargs):
            submit_cmd += " -l nodes=1:ppn={}".format(kwargs["cpus"])
        if __check("nodes", kwargs):
            submit_cmd += " -l nodes={}".format(kwargs["nodes"])

    if __check("partition", kwargs):
        submit_cmd += " -q {}".format(kwargs["partition"])
    if __check("job_name", kwargs):
        submit_cmd += " -N {}".format(kwargs["job_name"])
    if __check("depends_on", kwargs):
        submit_cmd += " -hold_jid {}".format(kwargs["depends_on"])
    if __check("email_address", kwargs):
        submit_cmd += " -M {}".format(kwargs["email_address"])
    if __check("email_options", kwargs):
        submit_cmd += " -m {}".format(__torque_e_opts(kwargs["email_options"]))
    if __check("input", kwargs):
        submit_cmd += " -i {}".format(kwargs["input"])
    if __check("output", kwargs):
        submit_cmd += " -o {}".format(kwargs["output"])
    if __check("error", kwargs):
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

    if get_backend() == "slurm":  # Format with slurm options
        sub_script = sub_command.format(script, __submit_slurm(**kwargs))
    elif get_backend() == "torque":  # Format with torque options
        sub_script = sub_command.format(script, __submit_torque(**kwargs))

    (stdout, stderr) = bash(sub_script)  # Actaully call the script using bash

    try:  # To parse the output based on expected successful submission result
        chunks = stdout.rstrip(".").split(" ")
        for chunk in chunks:
            if chunk.strip().isdigit():
                return(chunk.strip())  # First try to grab IDs from sentences

        if get_backend() == "slurm":  # If still here, try common output formats
            # Successfully submitted job <Job ID>
            return (stdout.split(" ")[-1].strip("\n"))
        if get_backend() == "torque":
            # <Job ID>.hostname.etc.etc
            return (stdout.split(".")[0])

        if stderr:
            print(stderr, file=stderr)

    except (ValueError, IndexError) as err:
        print("Could not capture Job ID! Dependency checks may fail!")
        print("Err: {}".format(err))
        return ("")


def __cancel_jobs_slurm(*args):
    for job in args:
        scancel(job)


def __cancel_jobs_torque(*args):
    for job in args:
        qdel(job)


def cancel_jobs(*args):
    if get_backend() == "slurm":
        __cancel_jobs_slurm(*args)
    elif get_backend() == "torque":
        __cancel_jobs_torque(*args)


def __cancel_suspended_jobs_slurm():
    (out, err) = squeue(" -u {} -l -h -t {} -o %i".format(get_username(), "S"))
    job_list = [out.splitlines()]
    scancel(" ".join(job_list))


def __cancel_suspended_jobs_torque():
    (out, err) = qstat(" -f")
    job_ids = []

    for job in out.split("\n\n"):
        # Each chunk is a job with each attribute listed on a line
        if "    euser = {}".format(get_username()) in job:
            if "   job_state = PD" in job:
                for line in job.split("\n"):
                    if "Job Id:" in line:
                        job_ids += [line.split(":")[-1].strip()]

    cancel_jobs(job_ids)


def cancel_suspended_jobs():
    if get_backend() == "slurm":
        __cancel_suspended_jobs_slurm()
    elif get_backend() == "torque":
        __cancel_suspended_jobs_torque()


def __cancel_running_jobs_slurm():
    (out, err) = squeue(" -u {} -l -h -t {} -o %i".format(get_username(), "R"))
    job_list = [out.splitlines()]
    scancel(" ".join(job_list))


def __cancel_running_jobs_torque():
    (out, err) = qstat(" -f")
    job_ids = []

    for job in out.split("\n\n"):
        # Each chunk is a job with each attribute listed on a line
        if "    euser = {}".format(get_username()) in job:
            if "   job_state = R" in job:
                for line in job.split("\n"):
                    if "Job Id:" in line:
                        job_ids += [line.split(":")[-1].strip()]

    cancel_jobs(job_ids)


def cancel_running_jobs():
    if get_backend() == "slurm":
        __cancel_running_jobs_slurm()
    elif get_backend() == "torque":
        __cancel_running_jobs_torque()


def __requeue_suspended_jobs_slurm():
    (out, err) = squeue(" -u {} -l -h -t {} -o %i".format(get_username(), "PD"))
    job_list = [out.splitlines()]
    scontrol("requeue " + " ".join(job_list))


def __requeue_suspended_jobs_torque():
    raise ("Requeue not supported by Torque")


def requeue_suspended_jobs():
    if get_backend() == "slurm":
        __requeue_suspended_jobs_slurm()
    elif get_backend() == "torque":
        __requeue_suspended_jobs_torque()


def __existing_jobs_slurm():
    # Return the job names of the Pending or Running jobs for this user
    (out, err) = squeue(" -h -o %j -u {}".format(get_username()))
    return (out.splitlines())


def __existing_jobs_torque():
    (out, err) = qstat(" -f")
    job_names = []

    for job in out.split("\n\n"):
        # Each chunk is a job with each attribute listed on a line
        if "    euser = {}".format(get_username()) in job:
            for line in job.split("\n"):
                if "Job_Name" in line:
                    job_names += [line.split("=")[-1].strip()]

    return (job_names)


def existing_jobs():
    if get_backend() == "slurm":
        return __existing_jobs_slurm()
    elif get_backend() == "torque":
        return __existing_jobs_torque()