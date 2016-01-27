#!/usr/bin/env python
from BamToSam import BAM2SAM


def main():
    directory = "/group/stevensdata1/pinus_armandii_wgs/" + \
        "Clean_Alignments/BAM_Format"
    converter = BAM2SAM(directory, 10, None, False)
    converter.run()


if __name__ == "__main__":
    main()
