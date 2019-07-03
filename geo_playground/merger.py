#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import pandas as pd


ROW_ID = 'row_id'
STORE_ID = 'store_id'
VAR1 = 'var1'
VAR2 = 'var2'
RATIO = 'ratio'
ADDRESS = 'address'
ADDRESS_NORM = 'address_norm'
VALUE = 'value'
OUTPUT_COLUMNS = [STORE_ID, VAR1, VAR2, RATIO]

DELIMITER = ';'
DECIMALS = "%.2f"

# https://trafficsignstore.com/abbreviations.html
ABBRS = {
    'st': 'street',
    'rd': 'road',
    'sq': 'square',
    'pl': 'place',
    'bl': 'bridge',
    'rte': 'route',
}

SEPARATOR = '@@@'


class MergerError(Exception):
    """Generic error for errors thrown by the merger script"""

    def __init__(self, message):
        super().__init__()
        self.msg = message


def merge(csv1_path, csv2_path, csv_out_path):
    """Merge the content of `csv1_path` and `csv2_path` and save the result to `csv_out_path`.

    After loading the content of `csv1_path` and `csv2_path` to two dataframes, the
    latter are joined based on the attribute ROW_ID, then the attribute RATIO is calculated
    as VAR1/VAR2. In case the value of df2.variable2 is 0, the ratio is set to -1. Finally,
    only the columns STORE_ID, VAR1, VAR2 and RATIO are retained and save to `csv_out_path`.

    :param csv1_path: path of the first CSV file
    :param csv2_path: path of the second CSV file
    :param csv_out_path: path of the output CSV file
    """
    df1 = prepare_df(csv1_path, var_name='variable1')
    df2 = prepare_df(csv2_path, var_name='variable2')

    if df1.shape[0] != df2.shape[0]:
        raise MergerError("Dataframes with different number of rows")

    df_merge = pd.merge(df1[[ROW_ID, STORE_ID, ADDRESS, ADDRESS_NORM, VALUE]],
                        df2[[ROW_ID, ADDRESS, ADDRESS_NORM, VALUE]],
                        on=ADDRESS_NORM)
    df_merge[RATIO] = df_merge.apply(lambda row: DECIMALS % (row.value_x / row.value_y) if row.value_y > 0 else -1,
                                     axis=1)
    df_merge = df_merge[[STORE_ID, VALUE + '_x', VALUE + '_y', RATIO]]
    df_merge.columns = [STORE_ID, VAR1, VAR2, RATIO]

    df_merge.to_csv(csv_out_path, sep=";", index=False)


def prepare_df(csv_path, var_name):
    """Prepare a dataframe.

    Read the content of `csv_path`, create and return a dataframe shaped as follows:
    ROW_ID, STORE_ID, ADDRESS,   VALUE
    1       1         1 High St. 74
    ..      ..        ..         ..
    3       3         2 Low St.  5

    In case the STORE_ID doesn't exist, it is replace with an empty string
    the VALUE attribute is based on `var_name`.

    :param csv_path: path of the CSV file
    :param var_name: variable column name

    :returns a dataframe
    """
    df = pd.read_csv(csv_path, sep=DELIMITER)

    df_norm = pd.DataFrame(columns=[ROW_ID, STORE_ID, ADDRESS, ADDRESS_NORM, VALUE])

    for index, row in df.iterrows():
        store_id = row.get('id_store', '')
        address = row['address']
        normalize_address = normalize(row['address'])
        var = row[var_name]

        item = {
            ROW_ID: index,
            STORE_ID: store_id,
            ADDRESS: address,
            ADDRESS_NORM: normalize_address,
            VALUE: float(var)
        }

        df_norm.loc[index] = item

    return df_norm


def normalize(address):
    """Normalize a given address by applying simple string manipulation operations.

    The operations applied to an address are the following:
    - convert the address to lowercase
    - replace non-alphanumeric chars with `SEPARATOR`
    - replace known address abbreviations (e.g., st., rd.) with full values
    - sort the address to simplify comparison with other addresses

    Find below some examples:
        - '100 High St.'         -> '100@@@high@@@street'
        - '382-384 Brixton Rd'   -> '382@@@384@@@brixton@@@rd'
        - '11, little stonegate' -> '11@@@little@@@stonegate'

    :param address: address info

    :returns a normalized version of the address
    """
    replacement = re.sub('\W+', SEPARATOR, address.lower())

    processed = []
    for p in replacement.split(SEPARATOR):
        if not p:
            continue

        if p in ABBRS:
            processed.append(ABBRS[p])
        else:
            processed.append(p)

    processed.sort()

    normalized = SEPARATOR.join(processed)
    return normalized
