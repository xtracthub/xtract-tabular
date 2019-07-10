import json
import os
import sys
import time
import unittest
from glob import glob
import argparse

sys.path.insert(0, '../')
from ex_columns import process_structured_file

cwd = os.getcwd() + '/test_files/'

if __name__ == "__main__" :

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename", default=None,
                        help="Filename")
    args = parser.parse_args()

    if args.filename is None:

        for filename in glob(cwd + '/*'):
            print("Processing : ", filename)
            start = time.time()
            metadata = str(process_structured_file(filename)[0])
            if metadata == None:
                print("This one failed")
            end = time.time()
            print("timer : {0}".format(end-start))

    else:
        filename = args.filename
        print("Processing : ", filename)
        start = time.time()
        metadata = str(process_structured_file(filename)[0])
        if metadata == None or metadata == 'None':
            print("This one failed")
        else:
            print("metadata: \n", metadata)
        end = time.time()
        print("timer : {0}".format(end-start))
