#!/usr/bin/env python
from cluster_commands import submit_job


def main():
    job = submit_job(command_str="ls -al ~",
               memory="2G",
               partition="bigmemh",
               cpus="4",
               email_address="cacampbell@ucdavis.edu",
               email_options="END,FAIL",
               output="test_cluster_commands.out",
               error="test_cluster_commands.err"
               )

    print(job)


if __name__ == "__main__":
    main()
