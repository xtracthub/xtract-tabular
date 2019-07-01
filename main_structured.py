import pandas as pd
import csv
import math
import argparse
import time
import multiprocessing as mp

# Minimum number of rows to analyze.
# Minimum number of mode values to include for non-numeric metadata.
MIN_ROWS = 5
MODE_COUNT = 10


def extract_columnar_metadata(filename, chunksize):
    """Get metadata from .csv files.

    Put more detailed explanation here.

    Parameters:
    filename (file path): Path to .csv file.

    Returns:
    grand_mdata (put type here): Place description here.
    """

    grand_mdata = {"physical": {}, "numeric": {}, "nonnumeric": {}}

    with open(filename, 'r') as data2:
        # Step 1. Quick scan for number of lines in file.
        line_count = 1
        for _ in data2:
            line_count += 1

        delimiter = get_delimiter(filename, line_count)

        # Step 3. Isolate the header data.
        header_info = get_header_info(data2, delim=delimiter)
        freetext_offset = header_info[0]
        header_col_labels = header_info[1]

        if header_col_labels is not None:
            grand_mdata["physical"]["headers"] = header_col_labels
        else:
            grand_mdata["physical"]["headers"] = None

        grand_mdata["physical"]["data_rows"] = line_count - freetext_offset
        grand_mdata["physical"]["total_rows"] = line_count

        # Step 4. Create dataframes from structured data.
        dataframes = get_dataframes(filename, chunksize=chunksize, delim=delimiter,
                                    skip_rows=freetext_offset + 1)

    data2.close()

    # Extract values from dataframe in parallel
    df_metadata = parallel_df_extraction(dataframes, header_col_labels, None)

    # Now REDUCE metadata by iterating over dataframes.
    # Numeric grand aggregates
    g_means = {}
    g_three_max = {}
    g_three_min = {}
    g_num_rows = {}

    # Nonnumeric grand aggregates
    g_modes = {}

    for md_piece in df_metadata:
        # First, we want to aggregate our individual pieces of
        #   NUMERIC metadata
        if "numeric" in md_piece:
            col_list = md_piece["numeric"]
            # For every column-level dict of numeric data...
            for col in col_list:
                # TODO: Create rolling update of mean, maxs, mins, Should, in theory, be faster
                # row_counts.
                if col["col_id"] not in g_means:
                    g_means[col["col_id"]] = col["metadata"]["mean"]

                    g_three_max[col["col_id"]] = {}
                    g_three_max[col["col_id"]]["max_n"] = col["metadata"][
                        "max_n"]

                    g_three_min[col["col_id"]] = {}
                    g_three_min[col["col_id"]]["min_n"] = col["metadata"][
                        "min_n"]

                    g_num_rows[col["col_id"]] = {}
                    g_num_rows[col["col_id"]]["num_rows"] = col["metadata"][
                        "num_rows"]

                else:
                    g_means[col["col_id"]] += col["metadata"]["mean"]
                    g_three_max[col["col_id"]]["max_n"].extend(
                        col["metadata"]["max_n"])
                    g_three_min[col["col_id"]]["min_n"].extend(
                        col["metadata"]["min_n"])
                    g_num_rows[col["col_id"]]["num_rows"] += col["metadata"][
                        "num_rows"]

        if "nonnumeric" in md_piece:

            col_list = md_piece["nonnumeric"]
            # Do the 'reduce' part of mapreduce.
            for col in col_list:  # 0 b/c held as single-elem list.
                for k in col:
                    col_modes = col[k]['top3_modes']

                    if k not in g_modes:
                        g_modes[k] = {'topn_modes': {}}

                    for mode_key in col_modes:
                        pass

                        if mode_key not in g_modes:
                            g_modes[k]['topn_modes'][mode_key] = {}
                            g_modes[k]['topn_modes'][mode_key] = col_modes[
                                mode_key]

                        else:
                            g_modes[k]['topn_modes'][mode_key] += col_modes[
                                mode_key]

    nonnum_count = len(g_modes)
    num_count = len(g_means)

    grand_mdata["physical"]["total_cols"] = nonnum_count + num_count

    # Just use the g_means key, because its keys must appear in all
    # summary stats anyways.
    for col_key in g_means:
        grand_mdata["numeric"][col_key] = {}
        grand_mdata["numeric"][col_key]["mean"] = float(
            g_means[col_key] / g_num_rows[col_key]["num_rows"])
        grand_mdata["numeric"][col_key]["num_rows"] = g_num_rows[col_key][
            "num_rows"]

        sorted_max = sorted(g_three_max[col_key]["max_n"], reverse=True)
        sorted_min = sorted(g_three_min[col_key]["min_n"], reverse=False)

        grand_mdata["numeric"][col_key]["max_n"] = sorted_max[:3]
        grand_mdata["numeric"][col_key]["min_n"] = sorted_min[:3]

    for col_key in g_modes:
        all_modes = g_modes[col_key]['topn_modes']

        top_modes = sorted(all_modes, key=all_modes.get, reverse=True)[
                    :MODE_COUNT]
        grand_mdata["nonnumeric"][col_key] = {}
        grand_mdata["nonnumeric"][col_key]['topn_modes'] = top_modes

    return grand_mdata


def extract_dataframe_metadata(df, header):
    """Extracts metadata from Panda dataframe.

    Extracts the number of rows, three largest values, three smallest
    values, and mean of each column of Panda dataframe. Additionally
    extracts mode of non-numeric data values.

    Parameters:
    df (Panda dataframe): Panda dataframe of .csv file.
    header (list(str)): List of fields of column headers.

    Return:
    df_metadata (dictionary(str : tuple)): Dictionary containing tuple
    of numeric and non-numeric metadata from Panda dataframe.
    """
    # Get only the numeric columns in data frame.
    ndf = df._get_numeric_data()

    # Get only the string columns in data frame.
    sdf = df.select_dtypes(include=[object])

    ndf_tuples = []

    for col in ndf:

        largest = df.nlargest(3, columns=col, keep='first')
        smallest = df.nsmallest(3, columns=col, keep='first')
        the_mean = ndf[col].mean()

        col_maxs = largest[col]
        col_mins = smallest[col]

        maxn = []
        minn = []
        for maxnum in col_maxs:
            maxn.append(maxnum)

        for minnum in col_mins:
            minn.append(minnum)

        if header is not None:
            ndf_tuple = {"col_id": header[col],
                         "metadata": {"num_rows": len(ndf), "min_n": minn,
                                      "max_n": maxn, "mean": the_mean}}
        else:
            ndf_tuple = {"col_id": "__{}__".format(col),
                         "metadata": {"num_rows": len(ndf), "min_n": minn,
                                      "max_n": maxn, "mean": the_mean}}
        ndf_tuples.append(ndf_tuple)

    # TODO: Repeated column names? They would just overwrite.
    nonnumeric_metadata = []
    top_modes = {}

    # Now get the nonnumeric data tags.
    for col in sdf:

        # Mode tags represent the three most prevalent values from each
        # paged dataframe.
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


def parallel_df_extraction(df, header, parallel):
    """Extracts dataframe metadata in parallel.

    Parameters:
    df (Panda dataframe): Panda dataframe.
    header (list(str)): List of fields in header of data columns.
    parallel (int): Number of processes to create to process dataframes.
    It is recommended that parallel <= number of CPU cores.

    Returns:
    combined_df_metadata (dictionary(str : tuple)): Dictionary
    containing tuple of numeric and non-numeric metadata from Panda
    dataframe.
    """
    pools = mp.Pool(processes=parallel)

    df_metadata = []
    for chunk in df:
        df_metadata = [pools.apply_async(extract_dataframe_metadata,
                                         args=(chunk, header))]
    combined_df_metadata = [p.get() for p in df_metadata]

    pools.close()
    pools.join()

    return combined_df_metadata


def get_delimiter(filename, numlines):
    """Finds delimiter in .csv file.

    Parameters:
    filename (file path): File path to .csv file.
    numlines (int): Number of lines in .csv file.

    Returns:
    delims[0] (str): Delimiter of .csv file.

    Raises:
    Non-Uniform Delimiter: Raises if delimiter within file is not
    constant.
    No columnds to parse from file: Raises if unable to turn .csv file
    into Panda dataframe.
    """
    # Step 1: Check whether filename can be converted to Panda dataframe
    try:
        pd.read_csv(filename, skiprows=numlines - MIN_ROWS,
                    error_bad_lines=False)
    except pd.errors.EmptyDataError:
        raise TypeError("No columns to parse from file")

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
            raise TypeError("Non-Uniform Delimiter")


# TODO: Check why header and dataframe_size are called then hardcoded
def get_dataframes(filename, delim, chunksize, skip_rows=0):
    """Creates a Panda dataframe from a .csv file.

    Parameters:
    filename (file path): File path to .csv file.
    header (str): Header for each column.
    delim (str): Delimiter of .csv file.
    skip_rows (int): Number of rows to skip over in .csv file to avoid
    preamble.
    dataframe_size (int): Size of each dataframe.

    Returns:
    iter_csv (Panda dataframe): Panda dataframe of filename.
    """
    iter_csv = pd.read_csv(filename, sep=delim, chunksize=chunksize, header=None,
                           skiprows=skip_rows,
                           error_bad_lines=False, iterator=True)

    return iter_csv


def count_fields(dataframe):
    """ Return the number of columns in our dataframe"""
    return dataframe.shape[1]


# Currently assuming short freetext headers.
def get_header_info(data, delim):
    """Retrieves length of preamble and headers of data columns in .csv
    file.

    Parameters:
    data (.csv file): .csv file.
    delim (str): Delimiter of data parameter (.csv file).

    Returns:
    preamble_length (int): Length of preamble.
    header (list(str)): List of fields in header of data columns.
    """
    data.seek(0)

    line_count = 0
    for _ in data:
        line_count += 1

    # Figure out the length of file via binary search)
    if line_count >= 5:  # Set min value or bin-search not useful.
        # A. Get the length of the preamble.
        preamble_length = _get_preamble(data, delim)

        # B. Determine whether the next line is a freetext header
        data.seek(0)

        header = None
        for i, line in enumerate(data):

            if preamble_length is None:
                header = None
                break
            if i == preamble_length:
                has_header = is_header_row(fields(line, delim))

                if has_header:  # == True
                    header = fields(line, delim)
                else:
                    header = None

            elif i > preamble_length:
                break

        return preamble_length, header


def is_header_row(row):
    """Determines whether a row is a header row by checking whether all
    fields are non-numeric.

    Parameters:
    row (list(str)): List of fields (str) in a row.

    Returns:
    (bool): Whether row is a header row.
    """
    for field in row:
        if is_number(field):
            return False
    return True


def _get_preamble(data, delim):
    """Finds the line number of the last line of free-text preamble of
    .csv file.

    Parameters:
    data (.csv file): .csv file with preamble.
    delim (str): Delimiter of data parameter (.csv file).

    Return:
    last_preamble_line_num (int): Line number of the last line of
    preamble.

    Restrictions:
    Currently can only find last line of preamble when delimiter is a
    comma or tab.
    """
    data.seek(0)
    max_nonzero_row = None
    max_nonzero_line_count = None
    last_preamble_line_num = None

    # Get number of delimited columns in last nonempty row and row
    # number
    delim_counts = {}
    for i, line in enumerate(data):
        cur_line_field_count = len(line.split(delim))

        if cur_line_field_count != 0:
            delim_counts[i] = cur_line_field_count
            max_nonzero_row = i
            max_nonzero_line_count = cur_line_field_count

    # Now if the last three values are all the same...
    if (delim_counts[max_nonzero_row] == delim_counts[max_nonzero_row - 1]
            == delim_counts[max_nonzero_row - 2]):
        # Now binary-search from the end to find the last row with that
        # number of columns.
        starting_row = math.floor(max_nonzero_row - 2) / 2
        last_preamble_line_num = _last_preamble_line_bin_search(
                                    delim_counts,
                                    max_nonzero_line_count,
                                    starting_row,
                                    upper_bd=0,
                                    lower_bd=max_nonzero_row - 2)

    return last_preamble_line_num


def _last_preamble_line_bin_search(field_cnt_dict, target_field_num, cur_row,
                                   upper_bd=None, lower_bd=None):
    """Performs binary search to find the last line number of preamble.

    Performs a binary search on dictionary to find the last line number
    of preamble. Preamble is differentiated from data by comparing
    the number of delimiters in each line.

    Parameters:
    field_cnt_dict (dictionary(int : int): Dictionary of line number
    paired with number of delimiters in line.
    target_field_num (int): Number of delimiters in non-preamble lines
    (lines of data).
    cur_row (float or int): Current line number in binary search.
    upper_bd (int): Upper boundary of binary search.
    lower_bd (int): Lower boundary of binary search.

    Returns:
    (int): Line number of last line of preamble.

    Restrictions:
    Currently can only perform a binary search when delimiter is a comma
    or tab.
    """
    cur_row = math.floor(cur_row)

    # Check current row and next two to see if they are all the target
    # value.
    if (field_cnt_dict[cur_row] == field_cnt_dict[cur_row + 1]
            == field_cnt_dict[cur_row + 2] == target_field_num):
        # If so, then we want to move up in the file.
        new_cur_row = cur_row - math.floor((cur_row - upper_bd) / 2)
        # If we're in the first row, we should return here.
        if cur_row == 1 and field_cnt_dict[cur_row - 1] \
                == field_cnt_dict[cur_row] \
                == target_field_num:
            return 0
        elif cur_row == 1 and field_cnt_dict[cur_row - 1] != target_field_num:
            return 1
        else:
            recurse = _last_preamble_line_bin_search(field_cnt_dict,
                                                     target_field_num,
                                                     new_cur_row,
                                                     upper_bd=upper_bd,
                                                     lower_bd=cur_row)
            return recurse
    elif field_cnt_dict[cur_row] \
            == field_cnt_dict[cur_row + 1] \
            == target_field_num:
        return cur_row + 1
    # If not, then we want to move down in the file.
    else:
        new_cur_row = cur_row + math.floor((lower_bd - cur_row) / 2)

        if cur_row == new_cur_row:
            return cur_row + 1

        recurse = _last_preamble_line_bin_search(field_cnt_dict,
                                                 target_field_num, new_cur_row,
                                                 upper_bd=cur_row,
                                                 lower_bd=lower_bd)
        return recurse


def fields(line, delim):
    """Splits a line along the delimiters into a list of fields.

    Parameters:
    line (str): Line from a .csv file.
    delim (str): Delimiter of .csv file.

    Returns:
    fields (list(str)): List of individual fields.
    """
    column_fields = [field.strip(' \n\r\t') for field in line.split(delim)]
    return column_fields


def is_number(field):
    """Determines whether a string is numeric by attempting to cast it
    as a float.

    Parameters:
    field (str): Field from a row of .csv file.

    Returns:
    (boolean): Whether field can be cast to a float.
    # """
    try:
        float(field)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    """Takes file paths from command line and returns metadata.

    Arguments:
    --path (File path): File path of .csv file.

    Returns:
    meta (insert type here): Metadata of .csv file.
    t1 - t0 (float): Time it took to retrieve .csv metadata.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--path', help='File system path to file.',
                        required=True)
    parser.add_argument('--chunksize', help='Number of rows to process at once.',
                        required=False, default=10000)

    args = parser.parse_args()
    t0 = time.time()
    meta = {"tabular": extract_columnar_metadata(args.path, args.chunksize)}
    print(meta)
    t1 = time.time()
    print(t1-t0)
