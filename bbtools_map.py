#!/usr/bin/env python
from BBToolsMap import BBMapper
from BBToolsMap_NoStats import BBMapperNoStats
from simple_argparse import run_with_args


def main(input_root, output_root, reference="reference.fa", stats=False,
         exclusions=None, cluster_options=None, dry_run=False, verbose=False):
    m = BBMapperNoStats(input_root, output_root, input_regex="*.fq.gz")

    if stats:
        m = BBMapper(input_root, output_root)

    m.reference = reference
    m.modules = ['java']
    m.exclusions = exclusions
    # The following three attributes constrain the files
    # m.input_regex = ".*"  # All files match this
    # m.read_regex = ".*_R1\.fq.*"  # Only Read 1 of 2 matches this
    m.extension = ".fq.gz"  # The file extension, stated explicitly
    m.dry_run = dry_run  # Actually execute commands or not: True or False
    m.verbose = verbose  # Print Statements throughout code: True or False
    if not cluster_options:
        m.cluster_options = {
            "memory": "220G",
            "nodes": "1",
            "cpus": "20",
            "partition": "bigmemh",
            "job_name": "Map_",
            "depends_on": "",
            "email_user": "cacampbell@ucdavis.edu",
            "email_options": "END,FAIL",
            "time": "0",
            "bash": "#!/usr/bin/env bash"
        }
    return (m.run())

if __name__ == "__main__":
    job_list = run_with_args(main)  # Runs main with arguments from argv
    print(job_list)
