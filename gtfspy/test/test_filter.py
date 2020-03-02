import os
import unittest
import sqlite3
import datetime
import pandas

from gtfspy.gtfs import GTFS
from gtfspy.filter import FilterExtract
from gtfspy.filter import remove_all_trips_fully_outside_buffer

from gtfspy.import_gtfs import import_gtfs
import hashlib


class TestGTFSFilter(unittest.TestCase):
    def setUp(self):
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.gtfs_source_dir_filter_test = os.path.join(self.gtfs_source_dir, "filter_test_feed/")

        # self.G = GTFS.from_directory_as_inmemory_db(self.gtfs_source_dir)

        # some preparations:
        self.fname = self.gtfs_source_dir + "/test_gtfs.sqlite"
        self.fname_copy = self.gtfs_source_dir + "/test_gtfs_copy.sqlite"
        self.fname_filter = self.gtfs_source_dir + "/test_gtfs_filter_test.sqlite"

        self._remove_temporary_files()
        self.assertFalse(os.path.exists(self.fname_copy))

        conn = sqlite3.connect(self.fname)
        import_gtfs(self.gtfs_source_dir, conn, preserve_connection=True, print_progress=False)
        conn_filter = sqlite3.connect(self.fname_filter)
        import_gtfs(
            self.gtfs_source_dir_filter_test,
            conn_filter,
            preserve_connection=True,
            print_progress=False,
        )

        self.G = GTFS(conn)
        self.G_filter_test = GTFS(conn_filter)

        self.hash_orig = hashlib.md5(open(self.fname, "rb").read()).hexdigest()

    def _remove_temporary_files(self):
        for fn in [self.fname, self.fname_copy, self.fname_filter]:
            if os.path.exists(fn) and os.path.isfile(fn):
                os.remove(fn)

    def tearDown(self):
        self._remove_temporary_files()

    def test_copy(self):
        # do a simple copy
        FilterExtract(self.G, self.fname_copy, update_metadata=False).create_filtered_copy()

        # check that the copying has been properly performed:
        hash_copy = hashlib.md5(open(self.fname_copy, "rb").read()).hexdigest()
        self.assertTrue(os.path.exists(self.fname_copy))
        self.assertEqual(self.hash_orig, hash_copy)

    def test_filter_change_metadata(self):
        # A simple test that changing update_metadata to True, does update some stuff:
        FilterExtract(self.G, self.fname_copy, update_metadata=True).create_filtered_copy()
        # check that the copying has been properly performed:
        hash_orig = hashlib.md5(open(self.fname, "rb").read()).hexdigest()
        hash_copy = hashlib.md5(open(self.fname_copy, "rb").read()).hexdigest()
        self.assertTrue(os.path.exists(self.fname_copy))
        self.assertNotEqual(hash_orig, hash_copy)
        os.remove(self.fname_copy)

    def test_filter_by_agency(self):
        FilterExtract(
            self.G, self.fname_copy, agency_ids_to_preserve=["DTA"]
        ).create_filtered_copy()
        hash_copy = hashlib.md5(open(self.fname_copy, "rb").read()).hexdigest()
        self.assertNotEqual(self.hash_orig, hash_copy)
        G_copy = GTFS(self.fname_copy)
        agency_table = G_copy.get_table("agencies")
        assert "EXA" not in agency_table["agency_id"].values, "EXA agency should not be preserved"
        assert "DTA" in agency_table["agency_id"].values, "DTA agency should be preserved"
        routes_table = G_copy.get_table("routes")
        assert (
            "EXR1" not in routes_table["route_id"].values
        ), "EXR1 route_id should not be preserved"
        assert "AB" in routes_table["route_id"].values, "AB route_id should be preserved"
        trips_table = G_copy.get_table("trips")
        assert "EXT1" not in trips_table["trip_id"].values, "EXR1 route_id should not be preserved"
        assert "AB1" in trips_table["trip_id"].values, "AB1 route_id should be preserved"
        calendar_table = G_copy.get_table("calendar")
        assert (
            "FULLW" in calendar_table["service_id"].values
        ), "FULLW service_id should be preserved"
        # stop_times
        stop_times_table = G_copy.get_table("stop_times")
        # 01:23:45 corresponds to 3600 + (32 * 60) + 45 [in day seconds]
        assert 3600 + (32 * 60) + 45 not in stop_times_table["arr_time"]
        os.remove(self.fname_copy)

    def test_filter_by_start_and_end_full_range(self):
        # untested tables with filtering: stops, shapes
        # test filtering by start and end time, copy full range
        FilterExtract(
            self.G,
            self.fname_copy,
            start_date="2007-01-01",
            end_date="2011-01-01",
            update_metadata=False,
        ).create_filtered_copy()
        G_copy = GTFS(self.fname_copy)
        dsut_end = G_copy.get_day_start_ut("2010-12-31")
        dsut_to_trip_I = G_copy.get_tripIs_within_range_by_dsut(dsut_end, dsut_end + 24 * 3600)
        self.assertGreater(len(dsut_to_trip_I), 0)
        os.remove(self.fname_copy)

    def test_filter_end_date_not_included(self):
        # the end date should not be included:
        FilterExtract(
            self.G, self.fname_copy, start_date="2007-01-02", end_date="2010-12-31"
        ).create_filtered_copy()

        hash_copy = hashlib.md5(open(self.fname_copy, "rb").read()).hexdigest()
        self.assertNotEqual(self.hash_orig, hash_copy)
        G_copy = GTFS(self.fname_copy)
        dsut_end = G_copy.get_day_start_ut("2010-12-31")
        dsut_to_trip_I = G_copy.get_tripIs_within_range_by_dsut(dsut_end, dsut_end + 24 * 3600)
        self.assertEqual(len(dsut_to_trip_I), 0)

        calendar_copy = G_copy.get_table("calendar")
        max_date_calendar = max(
            [datetime.datetime.strptime(el, "%Y-%m-%d") for el in calendar_copy["end_date"].values]
        )
        min_date_calendar = max(
            [
                datetime.datetime.strptime(el, "%Y-%m-%d")
                for el in calendar_copy["start_date"].values
            ]
        )
        end_date_not_included = datetime.datetime.strptime("2010-12-31", "%Y-%m-%d")
        start_date_not_included = datetime.datetime.strptime("2007-01-01", "%Y-%m-%d")
        self.assertLess(
            max_date_calendar,
            end_date_not_included,
            msg="the last date should not be included in calendar",
        )
        self.assertLess(start_date_not_included, min_date_calendar)
        os.remove(self.fname_copy)

    def test_filter_spatially(self):
        # test that the db is split by a given spatial boundary
        FilterExtract(
            self.G,
            self.fname_copy,
            buffer_lat=36.914893,
            buffer_lon=-116.76821,
            buffer_distance_km=50,
        ).create_filtered_copy()
        G_copy = GTFS(self.fname_copy)

        stops_table = G_copy.get_table("stops")
        self.assertNotIn("FUR_CREEK_RES", stops_table["stop_id"].values)
        self.assertIn("AMV", stops_table["stop_id"].values)
        self.assertEqual(len(stops_table["stop_id"].values), 8)

        conn_copy = sqlite3.connect(self.fname_copy)
        stop_ids_df = pandas.read_sql(
            "SELECT stop_id from stop_times "
            "left join stops "
            "on stops.stop_I = stop_times.stop_I",
            conn_copy,
        )
        stop_ids = stop_ids_df["stop_id"].values

        self.assertNotIn("FUR_CREEK_RES", stop_ids)
        self.assertIn("AMV", stop_ids)

        trips_table = G_copy.get_table("trips")
        self.assertNotIn("BFC1", trips_table["trip_id"].values)

        routes_table = G_copy.get_table("routes")
        self.assertNotIn("BFC", routes_table["route_id"].values)
        # cases:
        # whole trip excluded
        # whole route excluded
        # whole agency excluded
        # part of trip excluded
        # part of route excluded
        # part of agency excluded
        # not removing stops from a trip that returns into area

        # test higher-order removals
        # stop A preserved
        # -> stop B preserved
        # -> stop C preserved

    def test_filter_spatially_2(self):
        n_rows_before = {"routes": 4, "stop_times": 14, "trips": 4, "stops": 6, "shapes": 4}
        n_rows_after_1000 = {  # within "soft buffer" in the feed data
            "routes": 1,
            "stop_times": 2,
            "trips": 1,
            "stops": 2,
            "shapes": 0,
        }
        n_rows_after_3000 = {  # within "hard buffer" in the feed data
            "routes": len(["t1", "t3", "t4"]),
            "stop_times": 11,
            "trips": 4,
            "stops": len({"P", "H", "V", "L", "B"}),
            # for some reason, the first "shapes": 4
        }
        paris_lat = 48.832781
        paris_lon = 2.360734

        # SELECT_MIN_MAX_SHAPE_BREAKS_BY_TRIP_I_SQL = "SELECT trips.trip_I, shape_id, min(shape_break) as min_shape_break, max(shape_break) as max_shape_break FROM trips, stop_times WHERE trips.trip_I=stop_times.trip_I GROUP BY trips.trip_I"
        # trip_min_max_shape_seqs = pandas.read_sql(
        #     SELECT_MIN_MAX_SHAPE_BREAKS_BY_TRIP_I_SQL, self.G_filter_test.conn
        # )

        for distance_km, n_rows_after in zip([1000, 3000], [n_rows_after_1000, n_rows_after_3000]):
            try:
                os.remove(self.fname_copy)
            except FileNotFoundError:
                pass
            FilterExtract(
                self.G_filter_test,
                self.fname_copy,
                buffer_lat=paris_lat,
                buffer_lon=paris_lon,
                buffer_distance_km=distance_km,
            ).create_filtered_copy()
            for table_name, n_rows in n_rows_before.items():
                self.assertEqual(
                    len(self.G_filter_test.get_table(table_name)),
                    n_rows,
                    "Row counts before differ in " + table_name + ", distance: " + str(distance_km),
                )
            G_copy = GTFS(self.fname_copy)
            for table_name, n_rows in n_rows_after.items():
                table = G_copy.get_table(table_name)
                self.assertEqual(
                    len(table),
                    n_rows,
                    "Row counts after differ in "
                    + table_name
                    + ", distance: "
                    + str(distance_km)
                    + "\n"
                    + str(table),
                )

            # assert that stop_times are resequenced starting from one
            counts = pandas.read_sql(
                "SELECT count(*) FROM stop_times GROUP BY trip_I ORDER BY trip_I", G_copy.conn
            )
            max_values = pandas.read_sql(
                "SELECT max(seq) FROM stop_times GROUP BY trip_I ORDER BY trip_I", G_copy.conn
            )
            self.assertTrue((counts.values == max_values.values).all())

    def test_remove_all_trips_fully_outside_buffer(self):
        stops = self.G.stops()
        stop_1 = stops[stops["stop_I"] == 1]

        n_trips_before = len(self.G.get_table("trips"))

        remove_all_trips_fully_outside_buffer(
            self.G.conn, float(stop_1.lat), float(stop_1.lon), 100000
        )
        self.assertEqual(len(self.G.get_table("trips")), n_trips_before)

        # 0.002 (=max 2 meters from the stop), rounding errors can take place...
        remove_all_trips_fully_outside_buffer(
            self.G.conn, float(stop_1.lat), float(stop_1.lon), 0.002
        )
        self.assertEqual(len(self.G.get_table("trips")), 2)  # value "2" comes from the data
