from xtract_tabular_main import extract_columnar_metadata, get_delimiter, get_header_info, _get_preamble, _last_preamble_line_bin_search

choice_base = "tests/test_files/"


files = ["gdrive-serverless-batching.csv", "freetext", "freetext_header", "no_headers"]

# for choice_file in files:
#
#     print(f"Processing {choice_file}")
#
#     try:
#         f_path = choice_base + choice_file
#         delim = get_delimiter(f_path, 37)
#         print(f"Delimiter is {delim}")
#         assert(delim == ",")
#
#         with open(f_path, 'r') as f:
#             h_info = get_header_info(f, delim)
#             # pre_info = _get_preamble(f, delim)
#
#         print(h_info)
#     except Exception as e:
#         print(f"{choice_file} failed with exception: {e}")

no_headers = choice_base + "comma_delim"

with open(no_headers, 'r') as f:
    # _get_preamble(f, ',')
    hi = get_header_info(f, ',')
    print(hi)