import os
import sqlite3
import unittest
from datetime import datetime

from matplotlib.axes import Axes

from gtfspy.gtfs import GTFS
from gtfspy.import_gtfs import import_gtfs
from gtfspy.plots import plot_trip_counts_per_day


class TestPlots(unittest.TestCase):
    def setUp(self):
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.fname = self.gtfs_source_dir + "/test_gtfs.sqlite"
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
        # simple test
        ax = plot_trip_counts_per_day(
            self.G, highlight_dates=["2009-01-01"], highlight_date_labels=["test_date"]
        )
        # test with multiple dates and datetime
        dates = [datetime(2009, month=10, day=1), datetime(2010, month=10, day=1)]
        labels = ["test_date_1", "test_date_2"]
        ax = plot_trip_counts_per_day(self.G, highlight_dates=dates, highlight_date_labels=labels)
        assert isinstance(ax, Axes)
