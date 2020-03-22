import unittest
import os
import pandas as pd

from gtfspy import util


class TestUtil(unittest.TestCase):
    @staticmethod
    def _approximately_equal(a, b):
        return abs(a - b) / float(abs(a + b)) < 1e-2

    def test_ut_to_utc_datetime_str(self):
        time_ut = 1438300800 + 24299
        assert util.ut_to_utc_datetime_str(time_ut) == "Jul 31 2015 06:44:59"

    def test_wgs84_width(self):
        lat = 60.4192161560059
        lon = 25.3302955627441
        # Test going 100m east
        width = util.wgs84_width(100, lat)
        lon2 = lon + width
        d = util.wgs84_distance(lat, lon, lat, lon2)
        self.assertTrue(self._approximately_equal(d, 100))
        # Test going 100m south
        height = util.wgs84_height(100)
        lat2 = lat + height
        d = util.wgs84_distance(lat, lon, lat2, lon)
        self.assertTrue(self._approximately_equal(d, 100))

    def test_day_seconds_to_str_time(self):
        str_time = util.day_seconds_to_str_time(25 * 3600 + 59 * 60 + 10)
        self.assertTrue(str_time == "25:59:10", "the times can also go over 24 hours")
        str_time = util.day_seconds_to_str_time(0)
        self.assertTrue(str_time == "00:00:00", "day should start at 00:00")

    def test_txt_to_pandas(self):
        source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        txtnames = [
            "agency",
            "routes",
            "trips",
            "calendar",
            "calendar_dates",
            "stop_times",
            "stops",
            "shapes",
        ]
        df = util.source_csv_to_pandas(source_dir, txtnames[3])
        self.assertIsInstance(df, pd.DataFrame)
        source_zip = os.path.join(os.path.dirname(__file__), "test_data/test_gtfs.zip")
        df = util.source_csv_to_pandas(source_zip, txtnames[4])
        self.assertIsInstance(df, pd.DataFrame)

    def test_difference_of_pandas_dfs(self):
        dict1 = {"lat": [1, 2, 3, 4, 5], "lon": [5, 6, 7, 8, 9], "data": [123, 342, 345, 123, 543]}
        dict2 = {"lat": [6, 7, 3, 4, 5], "lon": [5, 6, 7, 8, 9], "data": [656, 12, 34, 1112, 43]}
        df1 = pd.DataFrame(dict1)
        df2 = pd.DataFrame(dict2)
        df = util.difference_of_pandas_dfs(df1, df2, ["lat", "lon"])
        self.assertEqual(len(df.index), 2)
