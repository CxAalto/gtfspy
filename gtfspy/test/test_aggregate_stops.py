import os
import unittest

from gtfspy import calc_transfers
from gtfspy.aggregate_stops import aggregate_stops_spatially
from gtfspy.gtfs import GTFS


class AggregateStopsTest(unittest.TestCase):
    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs = GTFS.from_directory_as_inmemory_db(os.path.join(os.path.dirname(__file__), "test_data"))

    def tearDown(self):
        pass

    def test_aggregate_stops_spatially_small_threshold(self):
        # The coordinates of this stop are identical to another stop in the test data.
        self.gtfs.add_stop("AMV-phantom-stop", "Amargosa Valley (Demo)", "Phantom_stop", "", 36.641496, -116.40094)
        calc_transfers.calc_transfers(self.gtfs.conn, threshold_meters=2)
        n_stops_before = len(self.gtfs.stops())
        aggregate_stops_spatially(self.gtfs, threshold_meters=1)
        n_stops_after = len(self.gtfs.stops())
        self.assertEqual(n_stops_before, n_stops_after + 1, "There should be one stop less (phantom stop)")

    def test_aggregate_stops_spatially_large_threshold(self):
        EARTH_RADIUS = 1000 * 1000
        calc_transfers.calc_transfers(self.gtfs.conn, threshold_meters=EARTH_RADIUS)
        aggregate_stops_spatially(self.gtfs, threshold_meters=EARTH_RADIUS)
        n_stops_after = len(self.gtfs.stops())
        self.assertEqual(n_stops_after, 1, "There should be only one stop remaining after aggregating stops")

        n_different_stops_in_stop_times = len(
            self.gtfs.execute_custom_query_pandas("SELECT count(DISTINCT stop_I) FROM stop_times"))
        self.assertEqual(n_different_stops_in_stop_times, 1, "There should be only one stop left!")
        stop_distances_after = self.gtfs.execute_custom_query_pandas("SELECT * FROM stop_distances")
        self.assertEqual(len(stop_distances_after), 0, "There should be no stop_distances left between only one stop!")
