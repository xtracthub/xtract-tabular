import xtract_tabular_main as ms
import os
import unittest

current_directory = os.getcwd()

comma_delim_path = current_directory + '/test_files/comma_delim'
freetext_header_path = current_directory + '/test_files/freetext_header'
no_headers_path = current_directory + '/test_files/no_headers'
tab_delim_path = current_directory + '/test_files/tab_delim'


# Test cases for xtract-tabular
class TabularTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_is_number(self):
        self.assertTrue(ms.is_number(5))
        self.assertTrue(ms.is_number("5"))
        self.assertFalse(ms.is_number("t"))

    def test_is_header_row(self):
        is_header_row_example_1 = ["this", "is", "a", "test", "field"]
        is_header_row_example_2 = ["this", "is", "false", 5]
        self.assertTrue(ms.is_header_row(is_header_row_example_1))
        self.assertFalse(ms.is_header_row(is_header_row_example_2))

    def test_get_delimiter(self):
        self.assertTrue(ms.get_delimiter("tests/test_files/comma_delim", 20)
                        , ",")
        self.assertTrue(ms.get_delimiter("tests/test_files/tab_delim", 20)
                        , "\t")

    def test_get_preamble(self):
        with open(freetext_header_path, "r") as _get_preamble_example_1:
            self.assertEqual(ms._get_preamble(_get_preamble_example_1, ",")
                             , 82)
            _get_preamble_example_1.close()

        with open(no_headers_path, "r") as _get_preamble_example_2:
            self.assertEqual(ms._get_preamble(_get_preamble_example_2, ","), 0)
            _get_preamble_example_2.close()

    def test_get_header_info(self):
        with open(comma_delim_path, "r") as get_header_info_example_1:
            self.assertEqual(ms.get_header_info(get_header_info_example_1, ",")
                             , (1, ['"Game Number"', '"Game Length"']))
            get_header_info_example_1.close()

        with open(freetext_header_path, "r") as get_header_info_example_2:
            self.assertEqual(ms.get_header_info(get_header_info_example_2, ",")
                             , (82, ['"LatD"', '"LatM"', '"LatS"', '"NS"',
                                     '"LonD"', '"LonM"', '"LonS"', '"EW"',
                                     '"City"', '"State"']))
            get_header_info_example_2.close()


if __name__ == "__main__":
    unittest.main()
