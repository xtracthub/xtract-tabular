import main_structured as ms
import os
import unittest

current_directory = os.getcwd()

comma_delim_path = current_directory + '/tests/test_files/comma_delim'
freetext_header_path = current_directory + '/tests/test_files/freetext_header'
# no_headers_path

class TabularTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_is_number(self):
        self.assertTrue(ms.is_number(5))
        self.assertTrue(ms.is_number("5"))
        self.assertFalse(ms.is_number("t"))

    def test_is_header_row(self):
        example_1 = ["this", "is", "a", "test", "field"]
        example_2 = ["this", "is", "false", 5]
        self.assertTrue(ms.is_header_row(example_1))
        self.assertFalse(ms.is_header_row(example_2))

    def test_get_delimiter(self):
        self.assertTrue(ms.get_delimiter("tests/test_files/comma_delim", 20), ",")
        self.assertTrue(ms.get_delimiter("tests/test_files/tab_delim",20), "\t")
	

print(ms.extract_columnar_metadata(comma_delim_path, 10000))

print(comma_delim_path)
if __name__ == "__main__":
    unittest.main()
