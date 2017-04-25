import os
import unittest

from gtfspy.gtfs import GTFS
from gtfspy.geometry import get_convex_hull_coordinates, get_approximate_convex_hull_area_km2, approximate_convex_hull_area

class GeometryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs = GTFS.from_directory_as_inmemory_db(self.gtfs_source_dir)

    def test_get_convex_hull_coordinates(self):
        lons, lats = get_convex_hull_coordinates(self.gtfs)
        self.assertEqual(len(lons), 5 + 1)
        self.assertEqual(len(lats), 5 + 1)
        for value in lons + lats:
            self.assertIsInstance(value, float)

    def test_get_convex_hull_area(self):
        area = get_approximate_convex_hull_area_km2(self.gtfs)
        print(area)
        self.assertGreater(area, 10)

    def test_approximate_convex_hull_area(self):
        # helsinki railway station, Helsinki
        # leppavaara station, Helsinki
        # pasila railway station, Helsinki
        leppavaara_coords = 60.219163, 24.813390
        pasila_coords = 60.199136, 24.934090
        main_railway_station_coords = 60.171545, 24.940734
        # lat, lon
        lats, lons = list(zip(leppavaara_coords, pasila_coords, main_railway_station_coords))
        approximate_reference = 9.91  # computed using https://asiointi.maanmittauslaitos.fi/karttapaikka/
        computed = approximate_convex_hull_area(lons, lats)
        self.assertTrue(approximate_reference * 0.9  < computed < approximate_reference * 1.1)


