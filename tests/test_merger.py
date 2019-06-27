#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import shutil
import tempfile
import unittest
import uuid

from geo_playground.merger import (merge,
                                   prepare_df,
                                   MergerError,
                                   DELIMITER,
                                   DECIMALS)


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


class TestMerger(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tmp_path = tempfile.mkdtemp(prefix='geo_playground')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_path)

    def test_prepare_df(self):
        """Test prepare_df function"""

        file_path = './data/data_poi_part_1.csv'
        var_name = 'variable1'

        with open(file_path) as f:
            lines = sum(1 for _ in f)

        df = prepare_df(file_path, var_name)
        count_row = df.shape[0] + 1  # add the header

        self.assertEqual(lines, count_row)

    def test_prepare_df_missing_attribute(self):
        """Test whether an error is thrown when an attribute doesn't exist"""

        file_path = './data/data_poi_part_1.csv'
        var_name = 'unknown'

        with self.assertRaises(KeyError):
            prepare_df(file_path, var_name)

    def test_merge(self):
        """Test merge function"""

        file_path1 = './data/data_poi_part_1.csv'
        file_path2 = './data/data_poi_part_2.csv'
        output_path = os.path.join(self.tmp_path, str(uuid.uuid4()))

        with open(file_path1) as f:
            rows_1 = [line for line in csv.reader(f, delimiter=DELIMITER)][1:]

        with open(file_path2) as f:
            rows_2 = [line for line in csv.reader(f, delimiter=DELIMITER)][1:]

        merge(file_path1, file_path2, output_path)

        with open(output_path) as f:
            rows_out = [line for line in csv.reader(f, delimiter=DELIMITER)][1:]

        self.assertEqual(len(rows_1), len(rows_out))

        for idx, row_1 in enumerate(rows_1[:-1]):
            ratio = DECIMALS % (int(row_1[-2]) / int(rows_2[idx][-1]))
            self.assertEqual(ratio, rows_out[idx][-1])

        idx = 4
        self.assertEqual('-1', rows_out[idx][-1])

    def test_merge_different_rows(self):
        """Test whether an exception is thrown when the dataframes have different sizes"""

        file_path1 = './data/data_poi_part_1.csv'
        file_path2 = './data/data_poi_part_2_less_rows.csv'
        output_path = os.path.join(self.tmp_path, str(uuid.uuid4()))

        with self.assertRaises(MergerError):
            merge(file_path1, file_path2, output_path)

    def test_merge_file_wrong_format(self):
        """Test whether an exception is thrown when a file is not a CSV one"""

        file_path1 = './data/data_poi_part_1_wrong_format'
        file_path2 = './data/data_poi_part_2.csv'
        output_path = os.path.join(self.tmp_path, str(uuid.uuid4()))

        with self.assertRaises(Exception):
            merge(file_path1, file_path2, output_path)

    def test_merge_file_not_found(self):
        """Test whether an exception is thrown when a file is not found"""

        file_path1 = './data/xxx.csv'
        file_path2 = './data/data_poi_part_1.csv'
        output_path = os.path.join(self.tmp_path, str(uuid.uuid4()))

        with self.assertRaises(FileNotFoundError):
            merge(file_path1, file_path2, output_path)


if __name__ == "__main__":
    unittest.main(warnings='ignore')
