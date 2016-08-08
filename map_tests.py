#!/usr/bin/env python3
from Bash import bash
from shlex import split
from sys import stderr


def main():
    speeds = ["vfast", "fast", "normal", "slow", "vslow"]
    modulos = ["--usemodulo"]
    stats = ["--stats", ""]
    reads = ["100", "10"]

    command = ("map.py --verbose --partition=bigmemm --memory={mem} --cpus=14 "
               "--email_address=cacampbell@ucdavis.edu --extension=.fastq.gz$ "
               "--email_options=BEGIN,FAIL,END --input_root=ErrCorrect.1 "
               "--output_root={outroot} --job_name={jobname} {modulo} --pigz "
               "--speed={speed} {stats} --read_groups --num_reads={reads} "
               "ref=pila.v1.0.scafSeq.fa nodisk")

    for i, speed in enumerate(speeds):
        for j, modulo in enumerate(modulos):
            for k, stat in enumerate(stats):
                for l, read in enumerate(reads):
                    mem = "300G"
                    if j == 0:
                        mem = "200G"

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

                    jobname = "Map_Test.{s}.{m}.{st}.{r}.".format(
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
                        outroot=outroot
                    ))

                    (out, err) = bash(*args)
                    print(out)
                    print(err, file=stderr)

if __name__ == "__main__":
    main()
