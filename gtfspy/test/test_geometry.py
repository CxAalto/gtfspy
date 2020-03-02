import os
import unittest
import numpy as np

from gtfspy.gtfs import GTFS
from gtfspy.geometry import (
    get_convex_hull_coordinates,
    get_approximate_convex_hull_area_km2,
    approximate_convex_hull_area,
    compute_buffered_area_of_stops,
)


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
        approximate_reference = (
            9.91  # computed using https://asiointi.maanmittauslaitos.fi/karttapaikka/
        )
        computed = approximate_convex_hull_area(lons, lats)
        self.assertTrue(approximate_reference * 0.9 < computed < approximate_reference * 1.1)

    def test_get_buffered_area_of_stops(self):
        # stop1 is far from stop2, theres no overlap
        # stop1 and stop3 are close and could have overlap
        # The area has an accuracy between 95%-99% of the real value.
        stop1_coords = 61.129094, 24.027896
        stop2_coords = 61.747408, 23.924279
        stop3_coords = 61.129621, 24.027363
        # lat, lon
        lats_1, lons_1 = list(zip(stop1_coords))
        lats_1_2, lons_1_2 = list(zip(stop1_coords, stop2_coords))
        lats_1_3, lons_1_3 = list(zip(stop1_coords, stop3_coords))

        # One point buffer
        buffer_onepoint = 100  # 100 meters of radius
        true_area = 10000 * np.pi  # area = pi * square radius
        area_1 = compute_buffered_area_of_stops(lats_1, lons_1, buffer_onepoint)
        confidence = true_area * 0.95
        self.assertTrue(confidence < area_1 < true_area)

        # Two points buffer non-overlap
        # Note: the points are "far away" to avoid overlap, but since they are points in the same city
        # a "really big buffer" could cause overlap and the test is going fail.
        buffer_nonoverlap = 100  # 100 meters of radius
        two_points_nonoverlap_true_area = (
            2 * buffer_nonoverlap ** 2 * np.pi
        )  # area = pi * square radius
        area_1_2 = compute_buffered_area_of_stops(lats_1_2, lons_1_2, buffer_nonoverlap)
        confidence_2 = two_points_nonoverlap_true_area * 0.95
        self.assertTrue(confidence_2 < area_1_2 and area_1_2 < two_points_nonoverlap_true_area)

        # Two points buffer with overlap
        # Points so close that will overlap with a radius of 100 meters
        buffer_overlap = 100  # 100 meters of radius
        area_1_3 = compute_buffered_area_of_stops(lats_1_3, lons_1_3, buffer_overlap)
        self.assertLess(area_1, area_1_3)
        self.assertLess(area_1_3, two_points_nonoverlap_true_area)

        # 'Half-overlap'
        from gtfspy.util import wgs84_distance

        lat1, lat3 = lats_1_3
        lon1, lon3 = lons_1_3

        distance = wgs84_distance(lat1, lon1, lat3, lon3)
        # just a little overlap
        buffer = distance / 2.0 + 1
        area_1_3b = compute_buffered_area_of_stops(lats_1_3, lons_1_3, buffer, resolution=100)
        one_point_true_area = np.pi * buffer ** 2
        self.assertLess(one_point_true_area * 1.5, area_1_3b)
        self.assertLess(area_1_3b, 2 * one_point_true_area)

        # no overlap
        buffer = distance / 2.0 - 1
        area_1_3b = compute_buffered_area_of_stops(lats_1_3, lons_1_3, buffer, resolution=100)
        two_points_nonoverlap_true_area = 2 * buffer ** 2 * np.pi
        self.assertGreater(area_1_3b, two_points_nonoverlap_true_area * 0.95)
        self.assertLess(area_1_3b, two_points_nonoverlap_true_area)
