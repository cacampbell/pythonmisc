#!/usr/bin/env python
from SamToBam import SAM2BAM


def main():
    directory = "/group/stevensdata1/pinus_armandii_wgs/" + \
        "Clean_Alignments/SAM_Format"
    converter = SAM2BAM(directory, 40, None, True)
    converter.run()


if __name__ == "__main__":
    main()
