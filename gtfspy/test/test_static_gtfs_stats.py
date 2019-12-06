from __future__ import unicode_literals

import os
import unittest

from gtfspy.gtfs import GTFS
from research.route_diversity.static_route_type_analyzer import StaticGTFSStats


class TestGTFS(unittest.TestCase):
    """
    Test data can be found under test_data/ directory.
    """

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs_source_dir = self.__class__.gtfs_source_dir
        self.gtfs = self.__class__.G
        day_start = self.gtfs.get_day_start_ut_span()[0]
        self.gsg = StaticGTFSStats(day_start, fname_or_conn=self.__class__.G.conn)

    def tearDown(self):
        """This method is run once after _each_ test method is executed"""
        pass

    def test_get_param_string(self):
        self.assertEqual(self.gsg.get_param_string(123, 123, 123), "ds-123_st-123_et-123_t-")
        trips = ["321", "432", "543"]
        self.assertEqual(self.gsg.get_param_string(123, 123, 123, trips=trips), "ds-123_st-123_et-123_t-321_432_543")

    def test_get_segments(self):
        self.assertEqual(len(self.gsg.get_segments().index), 13)
        self.assertEqual(len(self.gsg.get_segments(frequency_threshold=33).index), 1)
        self.assertEqual(len(self.gsg.get_segments(frequency_threshold=4).index), 9)

        self.assertEqual(len(self.gsg.get_segments(start_time=7*3600, end_time=24*3600, frequency_threshold=1).index),
                         13)
        self.assertEqual(len(self.gsg.get_segments(start_time=7*3600, end_time=24*3600, frequency_threshold=33).index),
                         0)
        self.assertEqual(len(self.gsg.get_segments(start_time=7*3600, end_time=24*3600, frequency_threshold=30).index),
                         1)
        self.assertEqual(len(self.gsg.get_segments(start_time=7*3600, end_time=24*3600, frequency_threshold=2).index),
                         9)

