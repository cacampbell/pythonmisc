#!/usr/bin/env python
import re
import sys
import pickle


record_search_file = 'file/containing/headers/to/find'
record_database_file = 'file/containing/all/redcords/to/be/searched'
output_file = 'where/you/want/the/new/file/file.csv'
records_to_search = []
records_to_find = []
wanted_records = []


def main():
    # Open header only file and read lines into list
    with open(record_search_file, "r+") as f:
        records_to_find = f.read().splitlines()

    # Open total record file and read lines into list
    with open(record_database_file, "r+") as f:
        records_to_find = f.read().splitlines()

    # For each header and for each line, if header in line, then add to list
    for header in records_to_find:
        for record in records_to_search:
            if re.search(header, record):  # ... if header in record:
                sys.stdout.write("\n%s\n" % record)
                wanted_records.append(record)

    # open output file, write each list item to a line
    with open(output_file, "r+") as f:
        for record in wanted_records:
            f.write("%s\n" % record)

    # also dump the list to a python object in the same location as output
    pickle.dump(wanted_records, re.sub(".csv$", ".pyc", output_file))


if __name__ == "__main__":
    main()
