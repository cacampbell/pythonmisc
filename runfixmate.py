#!/usr/bin/env python
from Fixmate import Fixmate


def main():
    directory = "/group/stevensdata1/pinus_armandii_wgs/Clean_Alignments/" + \
    "SAM_Format"
    fixmate = Fixmate(directory, None)
    fixmate.run()


if __name__ == "__main__":
    main()
