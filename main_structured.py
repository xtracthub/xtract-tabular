
import pandas as pd
import csv
import math
import argparse
import time

MIN_ROWS = 5

"""
 TODO LIST: 
 1. Be able to isolate preamble and header (if exists) in a file. Hold preamble as free text string. 
 2. Should be able to make up header values for columns if they don't already exist. 
 3. Get meaningful numeric metadata
 4. Get meaningful nonnumeric metadata (most commonly used field values). 
 5. Data sampling. 
 6. Handle 2-line headers. ('/home/skluzacek/pub8/oceans/VOS_Natalie_Schulte_Lines/NS2010_09.csv')
 

"""


class NonUniformDelimiter(Exception):
    """When the file cannot be pushed into a delimiter. """


def extract_columnar_metadata(filename):
    """Get metadata from column-formatted file.
            :param data: (str) a path to an unopened file.
            :param pass_fail: (bool) whether to exit after ascertaining file class
            :param lda_preamble: (bool) whether to collect the free-text preamble at the start of the file
            :param null_inference: (bool) whether to use the null inference model to remove nulls
            :param nulls: (list(int)) list of null indices
            :returns: (dict) ascertained metadata
            :raises: (ExtractionFailed) if the file cannot be read as a columnar file"""

    with open(filename, 'rU') as data2:
        # Step 1. Quick scan for number of lines in file.
        # print("[DEBUG] Getting number of lines. ")
        line_count = 1
        for _ in data2:
            line_count += 1

        # print(str(line_count) + " lines.")

        # Step 2. Determine the delimiter of the file.
        # print("[DEBUG] Getting delimiter data.")
        delimiter = get_delimiter(filename, line_count)
        # print("Delimiter is " + delimiter)

        # Step 3. Isolate the header data.
        # print("[DEBUG] Getting header data.")
        header_info = get_header_info(data2, delim=",")  #TODO: use max-fields of ',', ' ', or '\t'???
        freetext_offset = header_info[0]
        header_col_labels = header_info[1]

        # print(header_info)

        # print("[DEBUG] Successfully got header data!")

        # Step 4. Extract content-based metadata.
        # print("[DEBUG] Getting dataframes")
        if header_col_labels != None:
            # print("WE HAVE HEADERS!")
            # print(freetext_offset)
            dataframes = get_dataframes(filename, header=None, delim=delimiter, skip_rows=freetext_offset+1)
        else:
            # print("WE HAVE NO HEADERS!")
            dataframes = get_dataframes(filename, header=None, delim=delimiter, skip_rows=freetext_offset+1)
        # print("[DEBUG] Successfully got dataframes!")
    data2.close()

    # Now process each data frame.
    # print("[DEBUG] Extracting metadata using *m* processes...")

    # Iterate over each dataframe to extract values.
    df_metadata = []
    for df in dataframes:

        # TODO: Need # rows and metadata aggregation.
        metadata = extract_dataframe_metadata(df, header_col_labels)
        # print(metadata)

        df_metadata.append(metadata)

    grand_mdata = {"numeric": {}, "nonnumeric": {}}
    # Now REDUCE metadata by iterating over dataframes.

    g_means = {}
    g_three_max = {}
    g_three_min = {}
    g_num_rows = {}

    for md_piece in df_metadata:

        # *** First, we want to aggregate our individual pieces of NUMERIC metadata *** #
        if "numeric" in md_piece:
            col_list = md_piece["numeric"]
            # For every column-level dict of numeric data...

            for col in col_list:
                # TODO: Create rolling update of mean, maxs, mins, row_counts.
                if col["col_id"] not in g_means:
                    g_means[col["col_id"]] = col["metadata"]["mean"]

                    g_three_max[col["col_id"]] = {}
                    g_three_max[col["col_id"]]["max_n"] = col["metadata"]["max_n"]

                    g_three_min[col["col_id"]] = {}
                    g_three_min[col["col_id"]]["min_n"] = col["metadata"]["min_n"]

                    g_num_rows[col["col_id"]] = {}
                    g_num_rows[col["col_id"]]["num_rows"] = col["metadata"]["num_rows"]

                else:
                    g_means[col["col_id"]] += col["metadata"]["mean"]
                    g_three_max[col["col_id"]]["max_n"].extend(col["metadata"]["max_n"])
                    g_three_min[col["col_id"]]["min_n"].extend(col["metadata"]["min_n"])
                    g_num_rows[col["col_id"]]["num_rows"] += col["metadata"]["num_rows"]

        if "nonnumeric" in md_piece:
            pass

    for col_key in g_means:  # Just use the g_means key, because its keys must appear in all summary stats anyways.

        # print(col_key)

        grand_mdata["numeric"][col_key] = {}
        grand_mdata["numeric"][col_key]["mean"] = float(g_means[col_key]/g_num_rows[col_key]["num_rows"])
        grand_mdata["numeric"][col_key]["num_rows"] = g_num_rows[col_key]["num_rows"]

        sorted_max = sorted(g_three_max[col_key]["max_n"], reverse=True)
        sorted_min = sorted(g_three_min[col_key]["min_n"], reverse=False)

        grand_mdata["numeric"][col_key]["max_n"] = sorted_max[:3]
        grand_mdata["numeric"][col_key]["min_n"] = sorted_min[:3]

    # print(grand_mdata)
    # *** First, we want to aggregate our individual pieces of NUMERIC metadata *** #

    # TODO: Return the nonnumeric half as well.
    return grand_mdata


def extract_dataframe_metadata(df, header):

    # Get only the numeric columns in data frame.
    ndf = df._get_numeric_data()

    # Get only the string columns in data frame.
    sdf = df.select_dtypes(include=[object])

    ndf_tuples = []

    for col in ndf:

        largest = df.nlargest(3, columns=col, keep='first')  # Output dataframe ordered by col.
        smallest = df.nsmallest(3, columns=col, keep='first')
        the_mean = ndf[col].mean()

        # Use GROUP_BY and then MEAN.
        col_maxs = largest[col]
        col_mins = smallest[col]

        maxn = []
        minn = []
        for maxnum in col_maxs:
            maxn.append(maxnum)

        for minnum in col_mins:
            minn.append(minnum)

        # (header_name (or index), [max1, max2, max3], [min1, min2, min3], avg)
        if header is not None:
            ndf_tuple = {"col_id": header[col], "metadata": {"num_rows": len(ndf), "min_n": minn, "max_n": maxn, "mean": the_mean}}
        else:
            ndf_tuple = {"col_id": "__{}__".format(col), "metadata": {"num_rows": len(ndf), "min_n": minn, "max_n": maxn, "mean": the_mean}}
        ndf_tuples.append(ndf_tuple)

    # TODO: Repeated column names? They would just overwrite.
    nonnumeric_metadata = []
    top_modes = {}

    # Now get the nonnumeric data tags.
    for col in sdf:
        # Mode tags represent the three most prevalent values from each paged dataframe.
        nonnumeric_top_3_df = sdf[col].value_counts().head(3)

        col_modes = {}
        for row in nonnumeric_top_3_df.iteritems():

            col_modes[row[0]] = row[1]

        if header is not None:
            top_modes[header[col]] = {"top3_modes": col_modes}

        else:
            top_modes["__{}__".format(col)] = {"top3_modes": col_modes}

    nonnumeric_metadata.append(top_modes)
    df_metadata = {"numeric": ndf_tuples, "nonnumeric": nonnumeric_metadata}

    return df_metadata


def get_delimiter(filename, numlines):

    # Step 1: Load last min_lines into dataframe.  Just to ensure it can be done.
    pd.read_csv(filename, skiprows=numlines-MIN_ROWS, error_bad_lines=False)

    # Step 2: Get the delimiter of the last n lines.
    s = csv.Sniffer()
    with open(filename, 'r') as fil:
        i = 1
        delims = []
        for line in fil:
            if i > numlines - MIN_ROWS and ('=' not in line):
                delims.append(s.sniff(line).delimiter)
            i += 1

        if delims.count(delims[0]) == len(delims):
            return delims[0]
        else:
            raise NonUniformDelimiter("Error in get_delimiter")


def get_dataframes(filename, header, delim, skip_rows=0, dataframe_size = 1000):

    iter_csv = pd.read_csv(filename, sep=delim, chunksize=10, header=None, skiprows=skip_rows,
                           error_bad_lines=False, iterator=True)

    return iter_csv


def count_fields(dataframe):
    print(dataframe.shape[1])


# Currently assuming short freetext headers.
def get_header_info(data, delim):

    data.seek(0)
    # Get the line count.
    line_count = 0
    for _ in data:
        line_count += 1

    # Figure out the length of file via binary search (in"seek_preamble")
    if line_count >= 5:  # set arbitrary min value or bin-search not useful.
        # A. Get the length of the preamble.
        preamble_length = _get_preamble(data, delim)

        # print("P-length: " + str(preamble_length))
        # B. Determine whether the next line is a freetext header
        data.seek(0)

        header = None
        for i, line in enumerate(data):

            if preamble_length == None:
                header = None
                break
            if i == preamble_length:  # +1 since that's one after the preamble.
                # print("The header row is: " + str(line))

                has_header = is_header_row(fields(line, delim))
                if has_header:  # == True
                    header = fields(line, delim)
                else:
                    header = None

            elif i > preamble_length:
                break

        return preamble_length, header


def is_header_row(row):
    """Determine if row is a header row by checking that it contains no fields that are
    only numeric.
        :param row: (list(str)) list of fields in row
        :returns: (bool) whether row is a header row"""

    for field in row:
        if is_number(field):
            return False
    return True


def _get_preamble(data, delim):
    data.seek(0)
    delim = ','
    max_nonzero_row = None
    max_nonzero_line_count = None
    last_preamble_line_num = None

    # *** Get number of delimited columns in last nonempty row (and row number) *** #
    delim_counts = {}
    for i, line in enumerate(data):
        cur_line_field_count = len(line.split(delim))

        if cur_line_field_count != 0:
            delim_counts[i] = cur_line_field_count
            max_nonzero_row = i
            max_nonzero_line_count = cur_line_field_count

    # print(delim_counts)

    # [Weed out complicated cases] Now if the last three values are all the same...
    if delim_counts[max_nonzero_row] == delim_counts[max_nonzero_row - 1] == delim_counts[max_nonzero_row - 2]:
        # Now binary-search from the end to find the last row with that number of columns.
        starting_row = math.floor(max_nonzero_row - 2) / 2  # Start in middle of file for sanity.
        last_preamble_line_num = _last_preamble_line_bin_search(delim_counts, max_nonzero_line_count, starting_row,
                                                                upper_bd=0, lower_bd=max_nonzero_row - 2)

    return last_preamble_line_num


def _last_preamble_line_bin_search(field_cnt_dict, target_field_num, cur_row, upper_bd=None, lower_bd=None):

    # Check current row and next two to see if they are all the target value.
    cur_row = math.floor(cur_row)

    # If so, then we want to move up in the file.
    if field_cnt_dict[cur_row] == field_cnt_dict[cur_row+1] == field_cnt_dict[cur_row+2] == target_field_num:

        new_cur_row = cur_row - math.floor((cur_row - upper_bd)/2)

        # If we're in the first row, we should return here.
        if cur_row == 1 and field_cnt_dict[cur_row-1] == field_cnt_dict[cur_row] == target_field_num:
            return 0

        elif cur_row == 1 and field_cnt_dict[cur_row-1] != target_field_num:
            return 1

        else:
            recurse = _last_preamble_line_bin_search(field_cnt_dict, target_field_num, new_cur_row,
                                                     upper_bd=upper_bd, lower_bd=cur_row)
            return recurse

    elif field_cnt_dict[cur_row] == field_cnt_dict[cur_row+1] == target_field_num:
        return cur_row + 1

    # If not, then we want to move down in the file.
    else:
        new_cur_row = cur_row + math.floor((lower_bd - cur_row) / 2)

        if cur_row == new_cur_row:
            return cur_row + 1

        recurse = _last_preamble_line_bin_search(field_cnt_dict, target_field_num, new_cur_row,
                                                 upper_bd=cur_row, lower_bd=lower_bd)
        return recurse


def fields(line, delim):
    # if space-delimited, do not keep whitespace fields, otherwise do
    fields = [field.strip(' \n\r\t') for field in line.split(delim)]
    return fields


def is_number(field):
    """Determine if a string is a number by attempting to cast to it a float.
        :param field: (str) field
        :returns: (bool) whether field can be cast to a number"""

    try:
        float(field)
        return True
    except ValueError:
        return False


if __name__== "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--path', help='Absolute system path to file.', required=True)

    args = parser.parse_args()

    t0 = time.time()
    meta = extract_columnar_metadata(args.path)
    t1 = time.time()

    print(meta)
    print(t1-t0)
