from __future__ import print_function
from __future__ import unicode_literals

import os
import unittest
import tempfile as temp

import pandas as pd

from gtfspy.gtfs import GTFS
from gtfspy import stats


class StatsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs = GTFS.from_directory_as_inmemory_db(self.gtfs_source_dir)

    def test_write_stats_as_csv(self):
        testfile = temp.NamedTemporaryFile(mode='w+b')

        stats.write_stats_as_csv(self.gtfs, testfile.name)
        df = pd.read_csv(testfile.name)
        print('len is ' + str(len(df)))
        self.assertEqual(len(df), 1)

        stats.write_stats_as_csv(self.gtfs, testfile.name)
        df = pd.read_csv(testfile.name)
        self.assertEqual(len(df), 2)
        testfile.close()

    def test_get_stats(self):
        d = stats.get_stats(self.gtfs)
        self.assertTrue(isinstance(d, dict))

    def test_calc_and_store_stats(self):
        self.gtfs.meta['stats_calc_at_ut'] = None
        stats.update_stats(self.gtfs)
        self.assertTrue(isinstance(stats.get_stats(self.gtfs), dict))
        self.assertTrue(self.G.meta['stats_calc_at_ut'] is not None)

    def test_get_median_lat_lon_of_stops(self):
        lat, lon = stats.get_median_lat_lon_of_stops(self.gtfs)
        self.assertTrue(lat != lon, "probably median lat and median lon should not be equal for any real data set")
        self.assertTrue(isinstance(lat, float))
        self.assertTrue(isinstance(lon, float))

    def test_get_centroid_of_stops(self):
        lat, lon = stats.get_centroid_of_stops(self.gtfs)
        self.assertTrue(lat != lon, "probably centroid lat and lon should not be equal for any real data set")
        self.assertTrue(isinstance(lat, float))
        self.assertTrue(isinstance(lon, float))

    def test_trip_stats(self):
        df = stats.trip_stats(self.gtfs)
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertTrue(len(df.columns.values) > 0)
        self.assertTrue(len(df.index) > 0)
        # applying the function with results_by_mode = True, which should produce a dict
        df = stats.trip_stats(self.gtfs, True)
        self.assertTrue(isinstance(df, dict))
        # assuming test data only has one mode type
        self.assertTrue(len(df.keys()) == 1)
