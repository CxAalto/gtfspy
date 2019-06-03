import os
import unittest

from gtfspy import calc_transfers
from gtfspy.aggregate_stops import aggregate_stops_spatially, merge_stops_tables_multi
from gtfspy.gtfs import GTFS
from gtfspy.filter import FilterExtract
from gtfspy.util import timeit
import numpy as np


class AggregateStopsTest(unittest.TestCase):
    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs = GTFS.from_directory_as_inmemory_db(os.path.join(os.path.dirname(__file__), "test_data"))
        self.gtfs2 = GTFS.from_directory_as_inmemory_db(os.path.join(os.path.dirname(__file__), "test_data"))
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.fname = self.gtfs_source_dir + "/test_gtfs.sqlite"
        self.fname_copy = self.gtfs_source_dir + "/test_gtfs_copy.sqlite"

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

    def test_merge_stops_tables_multi_close_stop_added(self):
        # The coordinates of this stop are identical to another stop in the test data.
        gtfs2 = self.gtfs2
        self.gtfs.add_stop("AMV-phantom-stop", "Amargosa Valley (Demo)", "Phantom_stop", "", 36.641496, -116.40094)
        gtfs1 = self.gtfs
        n_stops_after1, n_stops_before1 = self._perform_merge_stops_tables_multi_test(gtfs1, gtfs2)
        self.assertEqual(n_stops_after1 + 1, n_stops_before1, "There should be one stop less (phantom stop)")

    def test_merge_stops_tables_multi_far_stop_added(self):
        # The coordinates of this stop are identical to another stop in the test data.
        gtfs2 = self.gtfs2
        self.gtfs.add_stop("AMV-phantom-stop", "Amargosa Valley (Demo)", "Phantom_stop", "", 0, 0)
        gtfs1 = self.gtfs
        n_stops_after1, n_stops_before1 = self._perform_merge_stops_tables_multi_test(gtfs1, gtfs2)
        self.assertEqual(n_stops_after1, n_stops_before1, "The dbs should have the same stops")

    def _perform_merge_stops_tables_multi_test(self, gtfs1, gtfs2):
        calc_transfers.calc_transfers(gtfs1.conn, threshold_meters=2)
        n_stops_before1 = len(gtfs1.stops())
        n_stops_before2 = len(gtfs2.stops())
        self.assertEqual(n_stops_before1, n_stops_before2 + 1, "There should be one stop less (phantom stop)")
        merge_stops_tables_multi([gtfs1, gtfs2], threshold_meters=1)
        n_stops_after1 = len(gtfs1.stops())
        n_stops_after2 = len(gtfs2.stops())
        self.assertEqual(n_stops_after1, n_stops_after2, "The dbs should have the same stops")
        return n_stops_after1, n_stops_before1

    def test_calc_transfers(self):

        s = 1000
        d = 0.01
        samp1 = np.random.uniform(low=-d, high=d, size=(s,))
        samp2 = np.random.uniform(low=-d, high=d, size=(s,))

        for i, (s1, s2) in enumerate(zip(samp1, samp2)):
            self.gtfs.add_stop(str(i), "Amargosa Valley (Demo)", "Phantom_stop", "", 36.641496+s1, -116.40094+s2)
        self._calc_transfers_new()
        print("n_stop_distances", len(self.gtfs.get_table("stop_distances").index))
        self.setUp()
        for i, (s1, s2) in enumerate(zip(samp1, samp2)):
            self.gtfs.add_stop(str(i), "Amargosa Valley (Demo)", "Phantom_stop", "", 36.641496+s1, -116.40094+s2)
        self._calc_transfers_old()
        print("n_stop_distances", len(self.gtfs.get_table("stop_distances").index))

    @timeit
    def _calc_transfers_old(self):
        calc_transfers.calc_transfers(self.gtfs.conn)

    @timeit
    def _calc_transfers_new(self):
        calc_transfers.calc_transfers_using_geopandas(self.gtfs.conn)
