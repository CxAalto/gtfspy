# reload(sys)
# -*- encoding: utf-8 -*-
import unittest
import os
from gtfspy.gtfs import GTFS
from gtfspy import import_validator as iv
import pandas as pd


class TestImportValidator(unittest.TestCase):
    def setUp(self):

        # create validator object using textfiles
        test_feed_dir = "test/test_data"
        test_feed_b_dir = "test/test_data/feed_b"
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.G_txt = GTFS.from_directory_as_inmemory_db([test_feed_dir, test_feed_b_dir])
        self.validator_object_txt = iv.ImportValidator([test_feed_dir, test_feed_b_dir], self.G_txt)

    def test_validator_objects(self):

        self.assertIsInstance(self.validator_object_txt, iv.ImportValidator)
        self.assertEqual(len(self.validator_object_txt.gtfs_sources), 2)

    def test_source_gtfsobj_comparison(self):
        self.validator_object_txt._validate_table_counts()

    def test_null_counts_in_gtfsobj(self):
        self.validator_object_txt._validate_no_nulls()
        self.validator_object_txt.get_warnings()
