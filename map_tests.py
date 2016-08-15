#!/usr/bin/env python3
from Bash import bash
from shlex import split
from sys import stderr
from sys import argv


def main(reference="reference.fa", job_prefix="Map_Tests"):
    speeds = ["vfast", "fast", "normal", "slow", "vslow"]
    modulos = ["--usemodulo"]
    stats = ["--stats", ""]
    reads = ["100"]

    command = ("map.py --verbose --partition=bigmemm --memory={mem} --cpus=14 "
               "--email_address=cacampbell@ucdavis.edu --extension=.fastq.gz$ "
               "--email_options=FAIL,END --input_root=ErrCorrect_Repair.1 "
               "--output_root={outroot} --job_name={jobname} {modulo} --pigz "
               "--speed={speed} {stats} --num_reads={reads} "
               "--reference={reference} --read_groups")

    for i, speed in enumerate(speeds):
        for j, modulo in enumerate(modulos):
            for k, stat in enumerate(stats):
                for l, read in enumerate(reads):
                    mem = "300G"

                    def __opts():
                        s = speed
                        m = "no_modulo"
                        st = "no_stats"
                        r = read

                        if j == 0:
                            m = "modulo"

                        if k == 0:
                            st = "stats"

                        return {'s':s, 'm':m, 'st':st, 'r':r}

                    jobname = job_prefix + ".{s}.{m}.{st}.{r}.".format(
                        **__opts()
                    )

                    outroot = "Map_Tests/Map_Test.{s}.{m}.{st}.{r}".format(
                        **__opts()
                    )

                    args = split(command.format(
                        speed=speed,
                        modulo=modulo,
                        stats=stat,
                        reads=read,
                        mem=mem,
                        jobname=jobname,
                        outroot=outroot,
                        reference=ref
                    ))

                    (out, err) = bash(*args)
                    print(out)
                    print(err, file=stderr)

if __name__ == "__main__":
    main(argv[1], argv[2])
