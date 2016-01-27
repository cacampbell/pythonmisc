# Authors: Arnaud Joly
#
# License: BSD 3 clause
from __future__ import unicode_literals

import os
import os.path as op
from time import sleep
import subprocess
from getpass import getuser

from nose import SkipTest
from nose.tools import assert_equal
from nose.tools import assert_raises
from nose.tools import assert_in

from clusterlib.scheduler import queued_or_running_jobs
from clusterlib.scheduler import submit
from clusterlib.scheduler import _which
from clusterlib.scheduler import _get_backend
from clusterlib._testing import TemporaryDirectory
from clusterlib._testing import skip_if_no_backend


def _check_job_id(command):
    """Perform a dispatch and return the job id."""
    # TODO: This utility function should be properly documented any made more
    # robust to be included in the scheduler module itself
    cmd_encoding = 'utf-8'
    output = subprocess.check_output(
        command.encode(cmd_encoding), shell=True).decode(cmd_encoding)
    if output.startswith(u'Your job '):
        job_id = output.split()[2]
    elif output.startswith(u'Submitted batch job '):
        job_id = output.split()[3]
    else:
        raise RuntimeError(
            u"Failed to parse job_id from command output:\n %s\ncmd:\n%s"
            % (command, output))
    return job_id


def test_auto_backend():
    """Check the backend detection logic."""
    original_env_backend = os.environ.get('CLUSTERLIB_BACKEND', None)
    if original_env_backend is not None:
        del os.environ['CLUSTERLIB_BACKEND']
    try:
        # Check detection when no environment variable is set.
        if _which('scontrol'):
            # SLURM should be detected
            assert_equal(_get_backend('auto'), 'slurm')
        elif _which('qmod'):
            # SGE should be detected
            assert_equal(_get_backend('auto'), 'sge')
        else:
            # No backend can be detected
            assert_raises(RuntimeError, _get_backend, 'auto')

        # Check the use of the environment variable
        os.environ['CLUSTERLIB_BACKEND'] = 'slurm'
        assert_equal(_get_backend('auto'), 'slurm')

        os.environ['CLUSTERLIB_BACKEND'] = 'sge'
        assert_equal(_get_backend('auto'), 'sge')
    finally:
        # Restore the previous environment
        if original_env_backend is None:
            del os.environ['CLUSTERLIB_BACKEND']
        else:
            os.environ['CLUSTERLIB_BACKEND'] = original_env_backend


def test_fixed_backend():
    """Check that it is possible to fix explicit backends (when valid)."""
    # Supported backends
    assert_equal(_get_backend('slurm'), 'slurm')
    assert_equal(_get_backend('sge'), 'sge')

    # Unsupported backend
    assert_raises(ValueError, _get_backend, 'hadoop')


@skip_if_no_backend
def test_log_output(n_trials=30):
    """Test that log output is uniform accross scheduler."""

    with TemporaryDirectory() as temp_folder:
        user = getuser()
        job_completed = False
        # Launch a sleepy SGE job
        job_name = 'ok_job'
        command = submit(job_command="echo ok", job_name=job_name,
                         time="700", memory=500,
                         log_directory=temp_folder)
        job_id = _check_job_id(command)

        try:
            for _ in range(n_trials):
                if job_name not in queued_or_running_jobs(user=user):
                    # job has completed, let's check the output
                    job_completed = True
                    filename = "%s.%s.txt" % (job_name, job_id)
                    assert_equal(os.listdir(temp_folder), [filename])
                    with open(op.join(temp_folder, filename)) as fhandle:
                        assert_equal(fhandle.read().strip(), "ok")
                    break
                else:
                    # Let's wait a bit before retrying
                    sleep(5)

        finally:
            # Make sure to clean up even if there is a failure
            if not job_completed:
                if _get_backend('auto') == 'slurm':
                    subprocess.call(["scancel", job_id])
                else:
                    subprocess.call(["qdel", job_id])
                raise AssertionError(
                    "job %s (%s) has not completed after 5min."
                    % (job_id, job_name))


def test_queued_or_running_jobs_nobackend():
    """Test queued or running whenever no backend is available."""
    # Note that we can't use _get_backend since the user might
    # have set the CLUSTERLIB_BACKEND environment variable.
    if _which('qmod') is None and _which('scontrol') is None:
        # No backend available, thus no running job
        assert_equal(queued_or_running_jobs(), [])
    else:
        raise SkipTest("A backend is installed")


@skip_if_no_backend
def check_job_name_queued_or_running(job_name):
    with TemporaryDirectory() as temp_folder:
        user = getuser()

        # Launch job
        command = submit(job_command="sleep 600", job_name=job_name,
                         time="700", memory=500, log_directory=temp_folder)
        job_id = _check_job_id(command)

        # Assert that the job has been launched
        try:
            running_jobs = queued_or_running_jobs(user=user)
            assert_in(job_name, running_jobs)
        finally:
            # Make sure to clean up even if there is a failure
            if _get_backend() == "slurm":
                subprocess.call(["scancel", job_id])

            elif _get_backend() == "sge":
                subprocess.call(["qdel", job_id])
            else:
                raise NotImplementedError("backend not implemented")


def test_queued_or_running_jobs():
    """Test queued or running job function on sge and slurm."""

    for job_name in ["test-sleepy-job", u'test-unicode-sl\xe9\xe9py-job']:
        yield check_job_name_queued_or_running, job_name


def test_submit():
    """Test submit formatting function."""
    assert_equal(
        submit(job_command="python main.py", backend="sge"),
        'echo \'#!/bin/bash\npython main.py\' | qsub -N "job" -l '
        'h_rt=24:00:00 -l h_vmem=4000M')

    assert_equal(
        submit(job_command="python main.py", backend="slurm"),
        "echo '#!/bin/bash\npython main.py' | sbatch --job-name=job "
        "--time=24:00:00 --mem=4000")

    assert_equal(
        submit(job_command="python main.py", email="test@test.com",
               email_options="beas", backend="sge"),
        'echo \'#!/bin/bash\npython main.py\' | qsub -N "job" '
        '-l h_rt=24:00:00 -l h_vmem=4000M -M test@test.com -m beas')

    assert_equal(
        submit(job_command="python main.py", log_directory="/path/test",
               backend="sge"),
        'echo \'#!/bin/bash\npython main.py\' | qsub -N "job" '
        '-l h_rt=24:00:00 -l h_vmem=4000M -j y '
        '-o \'/path/test/$JOB_NAME.$JOB_ID.txt\'')

    assert_equal(
        submit(job_command="python main.py", log_directory="/path/test",
               backend="slurm"),
        "echo \'#!/bin/bash\npython main.py\' | sbatch --job-name=job "
        "--time=24:00:00 --mem=4000 -o /path/test/job.%j.txt")

    assert_raises(ValueError, submit, job_command="", backend="unknown")
