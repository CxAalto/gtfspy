import os
import unittest

from gtfspy.gtfs import GTFS
from gtfspy.timetable_validator import TimetableValidator

class TestGTFSValidator(unittest.TestCase):

    def setUp(self):
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.G = GTFS.from_directory_as_inmemory_db(self.gtfs_source_dir)

    def test_compiles(self):
        validator = TimetableValidator(self.G)
        warnings = validator.get_warnings()
        warning_counts = warnings.get_warning_counts()
        assert len(warning_counts) > 0


