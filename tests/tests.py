
import json
import os
import sys
import time
import unittest

sys.path.insert(0, '../')
from ex_columns import process_structured_file

# Uncomment the top for Windows. Use the second for OSx/Linux
# cwd = os.getcwd() + '\\' + 'test_files' + '\\'
cwd = os.getcwd() + '/test_files/'


class ExtractionTests(unittest.TestCase):

    def setUp(self):
        pass

    def test_freetext_header(self):  # DONE.
        filename = cwd + 'freetext_header'
        t0 = time.time()
        metadata = str(process_structured_file(filename)[0])
        t1 = time.time()
        new_json = open_json(filename + '.json')
        self.assertEqual(metadata, new_json)
        print("Test 0: " + str(t1 - t0) + " seconds.")

    def test_tabs_commas_same_output(self):  # DONE.
        filename1 = cwd + 'tab_delim'
        filename2 = cwd + 'comma_delim'
        t0 = time.time()
        # Note: Below is hacky, but close enough for Elastic Search :)
        metadata1 = str(str(process_structured_file(filename1)[0]).replace('"', '')).replace(' ', '')
        metadata2 = str(str(process_structured_file(filename2)[0]).replace('"', '')).replace(' ', '')
        t1 = time.time()
        self.assertTrue(metadata1 == metadata2) #== str(open_json(filename2 + '.json').replace('"', '')).replace(' ', ''))
        print("Test 1: " + str(t1-t0) + " seconds.")

    def test_no_headers(self):  # DONE.
        filename = cwd + 'no_headers'
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(str(metadata), open_json(filename + '.json'))
        print("Test 2: " + str(t1 - t0) + " seconds.")

    def test_freetext_should_fail(self):  # DONE.
        filename = cwd + 'freetext'
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(metadata, None)
        print("Test 3: " + str(t1 - t0) + " seconds.")

    def test_compressed_should_fail(self):
        filename = cwd + 'tarball'
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(metadata, None)
        print("Test 4: " + str(t1 - t0) + " seconds.")

    def test_images_should_fail(self):  # DONE.
        filename = cwd + 'image'
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(metadata, None)
        print("Test 5: " + str(t1 - t0) + " seconds.")

    def test_netCDF_should_fail(self):  # DONE.
        filename = cwd + "netcdf"
        t0 = time.time()
        metadata = process_structured_file(filename)[0]
        t1 = time.time()
        self.assertEqual(metadata, None)
        print("Test 6: " + str(t1 - t0) + " seconds.")

def open_json(filename):
    with open(filename) as f:
        for line in f:
            #print line
            return line

if __name__ == '__main__':
    unittest.main()
