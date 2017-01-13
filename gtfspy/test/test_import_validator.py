# reload(sys)
# -*- encoding: utf-8 -*-
import unittest
import os
from gtfspy.gtfs import GTFS
from gtfspy import import_validator as iv

class TestImportValidator(unittest.TestCase):
    def setUp(self):
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.G = GTFS.from_directory_as_inmemory_db(self.gtfs_source_dir)
        self.validator_object = iv.import_validator(self.gtfs_source_dir, self.G)

    def validator_object_ok(self):
        assert isinstance(self.validator_object, iv.ImportValidator)
        assert len(self.validator_object.gtfs_sources) == 1