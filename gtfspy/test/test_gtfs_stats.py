import os
import unittest

from ..gtfs import GTFS
from ..stats import Stats


class GTFSStatsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs_source_dir = self.__class__.gtfs_source_dir
        self.gtfsstats = Stats(self.__class__.G)

    def test_write_stats_as_csv(self):
        import tempfile as temp
        import pandas as pd

        testfile = temp.NamedTemporaryFile(mode='w+b')

        self.gtfsstats.write_stats_as_csv(testfile.name)
        df = pd.read_csv(testfile.name)
        print 'len is ' + str(len(df))
        assert len(df) == 1

        self.gtfsstats.write_stats_as_csv(testfile.name)
        df = pd.read_csv(testfile.name)
        assert len(df) == 2
        testfile.close()

    def test_get_stats(self):
        d = self.gtfsstats.get_stats()
        assert isinstance(d, dict)

    def test_calc_and_store_stats(self):
        self.G.meta['stats_calc_at_ut'] = None
        stats = self.gtfsstats.update_stats()
        assert isinstance(self.gtfsstats.get_stats(), dict)
        assert self.G.meta['stats_calc_at_ut'] is not None

    def test_get_median_lat_lon_of_stops(self):
        lat, lon = self.gtfsstats.get_median_lat_lon_of_stops()
        assert lat != lon, "probably median lat and median lon should not be equal for any real data set"
        assert isinstance(lat, float)
        assert isinstance(lon, float)

    def test_get_centroid_of_stops(self):
        lat, lon = self.gtfsstats.get_centroid_of_stops()
        assert lat != lon, "probably centroid lat and lon should not be equal for any real data set"
        assert isinstance(lat, float)
        assert isinstance(lon, float)