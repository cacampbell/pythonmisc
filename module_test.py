#!/usr/bin/env python
import module_loader
import subprocess

if __name__ == "__main__":
    module_loader.module('load', 'java')
    process = subprocess.Popen(['java', '-version'], stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    (out, err) = process.communicate()
    print out, err
