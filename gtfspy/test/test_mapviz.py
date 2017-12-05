import os
import sqlite3
import unittest

from gtfspy.gtfs import GTFS
from gtfspy.import_gtfs import import_gtfs
from gtfspy.mapviz import plot_route_network_from_gtfs


class TestMapviz(unittest.TestCase):

    def setUp(self):
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data/filter_test_feed")
        self.fname = self.gtfs_source_dir + "/test_gtfs.sqlite"
        self._remove_temporary_files()
        conn = sqlite3.connect(self.fname)
        import_gtfs(self.gtfs_source_dir, conn, preserve_connection=True, print_progress=False)
        self.G = GTFS(conn)

    def _remove_temporary_files(self):
        for fn in [self.fname]:
            if os.path.exists(fn) and os.path.isfile(fn):
                os.remove(fn)

    def tearDown(self):
        self._remove_temporary_files()

    def test_plot_trip_counts_per_day(self):
        # simple "it compiles" tests:
        ax = plot_route_network_from_gtfs(self.G)
        ax = plot_route_network_from_gtfs(self.G, map_style="light_all")
        ax = plot_route_network_from_gtfs(self.G, map_style="dark_all")
        ax = plot_route_network_from_gtfs(self.G, map_style="rastertiles/voyager")
        # for interactive testing
        # from matplotlib import pyplot as plt
        # plt.show()
