import unittest
import os
from gtfspy.gtfs import GTFS
from gtfspy.import_validator import ImportValidator


class TestImportValidator(unittest.TestCase):
    def setUp(self):
        # create validator object using textfiles
        test_feed_dir = os.path.join(os.path.dirname(__file__), "test_data/")
        test_feed_b_dir = os.path.join(test_feed_dir, "feed_b")
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.G_txt = GTFS.from_directory_as_inmemory_db([test_feed_dir, test_feed_b_dir])
        self.import_validator = ImportValidator(
            [test_feed_dir, test_feed_b_dir], self.G_txt, verbose=False
        )

    def test_source_gtfsobj_comparison(self):
        self.import_validator._validate_table_row_counts()

    def test_null_counts_in_gtfsobj(self):
        self.import_validator._validate_no_null_values()

    def test_validate(self):
        warnings_container = self.import_validator.validate_and_get_warnings()
        stop_dist_warning_exists = False
        for warning, count in warnings_container.get_warning_counter().most_common():
            if "stop_distances" in warning:
                stop_dist_warning_exists = True
        assert stop_dist_warning_exists
