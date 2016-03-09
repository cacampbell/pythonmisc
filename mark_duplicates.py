from MarkDuplicates import MarkDuplicates
import sys


def main(input_, output_):
    md = MarkDuplicates(input_, output_)
    md.slurm_options['mem'] = '200G'
    md.slurm_options['cpus'] = '10'
    md.slurm_options['partition'] = 'bigmemm'
    md.slurm_options['mail-user'] = 'cacampbell@ucdavis.edu'
    md.modules = ['java']
    md.verbose = False
    md.dry_run = False
    md.read_marker = "pe"
    md.job_prefix = "MD_"
    md.input_suffix = ".bam"
    md.run()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])