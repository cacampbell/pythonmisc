#!/usr/bin/env python3
from Bash import bash


def main():
    speeds = ["vfast", "fast", "normal", "slow", "vslow"]
    modulos = ["--usemodulo", ""]
    stats = ["--stats", ""]
    reads = ["100", "1000", "10000"]

    command = ("map.py --verbose --partition=bigmemm --memory={mem} --cpus=14 "
               "--email_address=cacampbell@ucdavis.edu "
               "--email_options=BEGIN,FAIL,END --input_root=ErrCorrect "
               "--output_root={outroot} --job_name={jobname} {modulo} "
               "--speed={speed} {stats} --read_groups --num_reads={reads}")

    for speed in speeds:
        for modulo in modulos:
            for stat in stats:
                for read in reads:
                    mem = ""
                    if "--usemodulo" == modulo:
                        mem = "200G"
                    else:
                        mem = "300G"

                    jobname = "Pwal_Map_Test_{}.{}.{}.{}".format(
                        speed,
                        modulo.rstrip("--"),
                        stat.rstrip("--"),
                        read
                    )

                    outroot = "Mapped_Test_{}.{}.{}.{}".format(
                        speed,
                        modulo.rstrip("--"),
                        stat.rstrip("--"),
                        read
                    )

                    bash(command.format(
                        speed=speed,
                        modulo=modulo,
                        stats=stat,
                        reads=reads,
                        mem=mem,
                        jobname=jobname,
                        outroot=outroot
                    ))

if __name__ == "__main__":
    main()
