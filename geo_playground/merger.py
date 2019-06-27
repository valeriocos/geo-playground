#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

    df_merge = pd.merge(df1[[ROW_ID, STORE_ID, ADDRESS, VALUE]], df2[[ROW_ID, ADDRESS, VALUE]], on=ROW_ID)
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

    df_norm = pd.DataFrame(columns=[ROW_ID, STORE_ID, ADDRESS, VALUE])

    for index, row in df.iterrows():
        store_id = row.get('id_store', '')
        address = row['address']
        var = row[var_name]

        item = {
            ROW_ID: index,
            STORE_ID: store_id,
            ADDRESS: address,
            VALUE: float(var)
        }

        df_norm.loc[index] = item

    return df_norm
