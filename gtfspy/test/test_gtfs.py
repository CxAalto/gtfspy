import datetime
import os
import sqlite3
import unittest

import networkx
import pandas

from gtfspy.gtfs import GTFS
from gtfspy.util import wgs84_distance


class TestGTFS(unittest.TestCase):
    """
    Test data can be found under test_data/ directory.
    """

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs_source_dir = self.__class__.gtfs_source_dir
        self.G = self.__class__.G
        # GTFS.from_directory_as_inmemory_db(self.gtfs_source_dir)
        # os.path.join(os.path.dirname(__file__), "test_data")

    def tearDown(self):
        """This method is run once after _each_ test method is executed"""
        pass

    def test_get_cursor(self):
        assert isinstance(self.G.get_cursor(), sqlite3.Cursor)

    def test_tzset(self):
        """ How to test this properly?"""
        pass

    def test_get_timezone_name(self):
        assert isinstance(self.G.get_timezone_string(), str)

    def test_get_day_start_ut(self):
        """ America/Los_Angeles on 2016,1,1 should map to -07:00 when DST IS in place"""
        date = datetime.datetime(2016, 6, 15)
        epoch = datetime.datetime(1970, 1, 1)
        day_start_utc_ut = (date - epoch).total_seconds()
        # day starts 7 hours later in Los Angeles
        day_start_ut_test = day_start_utc_ut + 7 * 3600
        day_start_ut_code = self.G.get_day_start_ut(date)
        # print day_start_ut_test - day_start_ut_code
        assert day_start_ut_test == day_start_ut_code

    def test_get_main_database_path(self):
        assert self.G.get_main_database_path() == "", "in memory database should equal ''"

        from gtfspy.import_gtfs import import_gtfs
        try:
            fname = self.gtfs_source_dir + "/test_gtfs.sqlite"
            if os.path.exists(fname) and os.path.isfile(fname):
                os.remove(fname)
            conn = sqlite3.connect(fname)
            import_gtfs(self.gtfs_source_dir, conn, preserve_connection=True, print_progress=False)
            G = GTFS(conn)
            assert os.path.exists(G.get_main_database_path())
            assert "/test_gtfs.sqlite" in G.get_main_database_path(), "path should be correct"
        finally:
            if os.path.exists(fname) and os.path.isfile(fname):
                os.remove(fname)

    def test_get_table(self):
        df = self.G.get_table("agencies")
        assert isinstance(df, pandas.DataFrame)

    def test_get_table_names(self):
        tables = self.G.get_table_names()
        assert isinstance(tables, list)
        assert len(tables) > 11, "quite many tables should be available"
        assert "routes" in tables

    def test_get_all_route_shapes(self):
        res = self.G.get_all_route_shapes()
        assert isinstance(res, list)
        el = res[0]
        keys = "name type agency lats lons".split()
        for key in keys:
            assert key in el
        for el in res:
            assert isinstance(el["name"], (str, unicode))
            assert isinstance(el["type"], int)
            assert isinstance(el["agency"], (str, unicode))
            assert isinstance(el["lats"], list), type(el['lats'])
            assert isinstance(el["lons"], list)
            assert isinstance(el['lats'][0], float)
            assert isinstance(el['lons'][0], float)

    def test_get_shape_distance_between_stops(self):
        # tested as a part of test_to_directed_graph, although this could be made a separate test as well
        pass


    def test_get_timezone_string(self):
        tz_string = self.G.get_timezone_string()
        assert len(tz_string) == 5
        assert tz_string[0] in "+-"
        for i in range(1, 5):
            assert tz_string[i] in "0123456789"
        # print tz_name, tz_string
        dt = datetime.datetime(1970, 1, 1)
        tz_string_epoch = self.G.get_timezone_string(dt)
        assert tz_string != tz_string_epoch

    def test_timezone_convertions(self):
        """
        Two methods are tested:
            ut_seconds_to_gtfs_datetime
            unlocalized_datetime_to_ut_seconds
        """
        ut = 10.0
        gtfs_dt = self.G.unixtime_seconds_to_gtfs_datetime(ut)
        # print gtfs_dt
        unloc_dt = gtfs_dt.replace(tzinfo=None)
        # print unloc_dt
        ut_later = self.G.unlocalized_datetime_to_ut_seconds(unloc_dt)
        # print ut_later
        assert ut == ut_later

    def test_get_trip_trajectory_data_within_timespan(self):
        # untested, really
        s, e = self.G.get_conservative_gtfs_time_span_in_ut()
        res = self.G.get_trip_trajectories_within_timespan(s, s + 3600 * 24)
        assert isinstance(res, dict)

    def test_get_stop_count_data(self):
        start_ut, end_ut = self.G.get_conservative_gtfs_time_span_in_ut()
        df = self.G.get_stop_count_data(start_ut, end_ut)
        assert isinstance(df, pandas.DataFrame)
        columns = ["stop_I", "count", "lat", "lon", "name"]
        for c in columns:
            assert c in df.columns
            el = df[c].iloc[0]
            if c in ["stop_I", "count"]:
                assert isinstance(el,  int)
            if c in ["lat", "lon"]:
                assert isinstance(el, float)
            if c in ["name"]:
                assert isinstance(el, (str, unicode)), type(el)
        assert (df['count'].values > 0).any()

    def test_get_segment_count_data(self):
        s, e = self.G.get_conservative_gtfs_time_span_in_ut()
        res = self.G.get_segment_count_data(s, e, use_shapes=True)
        assert len(res) > 0
        assert res is not None, "this is a it compiles test"

    def test_get_tripIs_active_in_range(self):
        # one line in calendar.txt:
        #  FULLW,1,1,1,1,1,1,1,20070101,20101231
        # one line in trips.txt
        #  AB,FULLW,AB1,to Bullfrog,0,1,
        # lines in stop_times.txt
        #  AB1, 8:00:00, 8:00:00, BEATTY_AIRPORT,1,,,,
        #  AB1, 8:10:00, 8:15:00, BULLFROG, 2,,,,
        dt_start_query = datetime.datetime(2007, 1, 1, 7, 59, 59)
        dt_end_query = datetime.datetime(2007, 1, 1, 8, 2, 01)
        dt_start_real = datetime.datetime(2007, 1, 1, 8, 0, 00)
        dt_end_real = datetime.datetime(2007, 1, 1, 8, 10, 00)
        start_query = self.G.unlocalized_datetime_to_ut_seconds(dt_start_query)
        end_query = self.G.unlocalized_datetime_to_ut_seconds(dt_end_query)
        start_real = self.G.unlocalized_datetime_to_ut_seconds(dt_start_real)
        end_real = self.G.unlocalized_datetime_to_ut_seconds(dt_end_real)

        found = False
        df = self.G.get_tripIs_active_in_range(start_query, end_query)
        for row in df.itertuples():
            assert (row.start_time_ut <= end_query) and \
                   (row.end_time_ut >= start_query), "some trip does not overlap!"
            if row.start_time_ut == start_real and row.end_time_ut == end_real:
                found = True
                # check that overlaps
        # for debugging::
        # if not found:
        #     print start_real, end_real
        #     for h in header:
        #         print h + " |",
        #     print ""
        #     for row in rows:
        #         for el in row:
        #             print el,
        #         print ""
        assert found, "a trip that should be found is not found"

    def test_get_trip_counts_per_day(self):
        df = self.G.get_trip_counts_per_day()
        columns = "dates trip_counts".split(" ")
        for c in columns:
            assert c in df.columns
        el = df.iloc[0]
        assert isinstance(el["dates"], (str, unicode))
        assert isinstance(el["trip_counts"], int)

    def test_get_spreading_trips(self):
        pass  # untested

    def test_get_closest_stop(self):
        # First row in test_data:
        # FUR_CREEK_RES, Furnace Creek Resort (Demo),, 36.425288, -117.133162,,
        lat_s, lon_s = 36.425288, -117.133162
        lat, lon = lat_s + 10**-5, lon_s + 10**-5
        stop_I = self.G.get_closest_stop(lat, lon)
        assert isinstance(stop_I, int)
        df = self.G.stop(stop_I)
        name = df['name'][0]
        # print name
        # check that correct stop has been found:
        assert name == "Furnace Creek Resort (Demo)"
        # distance to the stop should be below 50 meters for such a small separation:
        assert wgs84_distance(lat, lon, lat_s, lon_s) < 50

    def test_get_route_name_and_type_of_tripI(self):
        # just a simple random test:
        trip_I = 1
        name, type_ = self.G.get_route_name_and_type_of_tripI(trip_I)
        assert isinstance(name, unicode)
        assert isinstance(type_, int)

    def get_trip_stop_time_data(self):
        start_ut, end_ut = self.G.get_conservative_gtfs_time_span_in_ut()
        dsut_dict = self.G.get_tripIs_within_range_by_dsut(start_ut, end_ut)
        dsut, trip_Is = dsut_dict.items()[0]
        df = self.G.get_trip_stop_time_data(trip_Is[0], dsut)
        assert isinstance(df, pandas.DataFrame)
        columns = "departure_time_ut lat lon seq shape_break".split(" ")
        el = df.iloc[0]
        for c in columns:
            assert c in df.columns
            if c in "departure_time_ut lat lon".split(" "):
                assert isinstance(el[c], float)
            if c in "seq shape_break".split(" "):
                assert isinstance(el[c], int)

    def test_get_straight_line_transfer_distances(self):
        data = self.G.get_straight_line_transfer_distances()
        assert len(data) > 0
        a_stop_I = None
        for index, row in data.iterrows():
            assert row['from_stop_I'] is not None
            a_stop_I = row['from_stop_I']
            assert row['to_stop_I'] is not None
            assert row['d'] is not None
        data = self.G.get_straight_line_transfer_distances(a_stop_I)
        assert len(data) > 0

    def test_get_conservative_gtfs_time_span_in_ut(self):
        start_ut, end_ut = self.G.get_conservative_gtfs_time_span_in_ut()
        start_dt = datetime.datetime(2007, 1, 1)
        start_ut_comp = self.G.unlocalized_datetime_to_ut_seconds(start_dt)
        end_dt = datetime.datetime(2010, 12, 31)
        end_ut_comp = self.G.unlocalized_datetime_to_ut_seconds(end_dt) + (28 * 3600)
        assert start_ut == start_ut_comp
        assert end_ut == end_ut_comp

    def test_get_location_name(self):
        location_name = self.G.get_location_name()
        self.assertEqual(location_name, "test_data")
        self.assertTrue(isinstance(location_name, (str, unicode) ), type(location_name))
        self.assertGreater(len(location_name), 0)
