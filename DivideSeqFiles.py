#!/usr/bin/env python
import os
import shutil


root_directory = "/Users/sara/Accession_sequencing/Before_trimming"
prefixes = []  # Will hold file prefixes, the sample name
directory_list = []  # Will hold directories


for item in os.listdir(root_directory):
    directory = os.path.join(root_directory, item)

    if os.path.isdir(directory):
        prefix = item[:6]  # Prefix (sample name)
        prefixes.append(prefix)
        directory_list.append(directory)


prefixes_ = prefixes[:]  # Make a copy
for prefix in set(prefixes_):  # For each unique prefix
    try:
        sample = os.path.join(root_directory, prefix)
        os.makedirs(sample)  # Make the unique directories (samples)
    except Exception as e:
        print "Could not make directory: %s. Exception: %s" % (sample, str(e))

for index, directory in enumerate(directory_list):
    try:
        sample = os.path.join(root_directory, prefixes[index])
	shutil.move(directory, sample)  # Move each directory to its respective sample
    except Exception as e:
        print "Could not move diectory %s to %s. Exception: %s" \
            % (directory, sample, str(e))
