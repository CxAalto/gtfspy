import os
import unittest
import sqlite3
import datetime

from gtfspy.gtfs import GTFS
from gtfspy.filter import filter_extract
from gtfspy.import_gtfs import import_gtfs
import hashlib

class TestGTFSfilter(unittest.TestCase):

    def setUp(self):
        self.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.G = GTFS.from_directory_as_inmemory_db(self.gtfs_source_dir)
        self.copy_fname = os.path.join(os.path.dirname(__file__), "test_copy/filtered.sqlite")
        self.copy_fname = os.path.join(os.path.dirname(__file__), "test_copy/filtered.sqlite")

    def test_filter_extract(self):

        try:
            # some preparations:
            fname = self.gtfs_source_dir + "/test_gtfs.sqlite"
            fname_copy = self.gtfs_source_dir + "/test_gtfs_copy.sqlite"
            for fn in [fname, fname_copy]:
                if os.path.exists(fn) and os.path.isfile(fn):
                    os.remove(fn)
            assert not os.path.exists(fname_copy)
            conn = sqlite3.connect(fname)

            import_gtfs(self.gtfs_source_dir, conn, preserve_connection=True, print_progress=False)

            # do a simple copy
            G = GTFS(conn)
            filter_extract(G, fname_copy, update_metadata=False)
            # check that the copying has been properly performed:
            hash_orig = hashlib.md5(open(fname, 'rb').read()).hexdigest()
            hash_copy = hashlib.md5(open(fname_copy, 'rb').read()).hexdigest()
            assert os.path.exists(fname_copy)
            assert hash_orig == hash_copy
            os.remove(fname_copy)

            # A simple test that changing update_metadata to True, does update somestuff:
            filter_extract(G, fname_copy, update_metadata=True)
            # check that the copying has been properly performed:
            hash_copy = hashlib.md5(open(fname_copy, 'rb').read()).hexdigest()
            assert os.path.exists(fname_copy)
            assert hash_orig != hash_copy
            os.remove(fname_copy)

            # test filtering by agency:
            filter_extract(G, fname_copy, agency_ids_to_preserve=['DTA'])
            hash_copy = hashlib.md5(open(fname_copy, 'rb').read()).hexdigest()
            assert hash_orig != hash_copy
            G_copy = GTFS(fname_copy)
            agency_table = G_copy.get_table("agencies")
            assert "EXA" not in agency_table['agency_id'].values, "EXA agency should not be preserved"
            assert "DTA" in agency_table['agency_id'].values, "DTA agency should be preserved"
            routes_table = G_copy.get_table("routes")
            assert "EXR1" not in routes_table['route_id'].values, "EXR1 route_id should not be preserved"
            assert "AB" in routes_table['route_id'].values, "AB route_id should be preserved"
            trips_table = G_copy.get_table("trips")
            assert "EXT1" not in trips_table['trip_id'].values, "EXR1 route_id should not be preserved"
            assert "AB1" in trips_table['trip_id'].values, "AB1 route_id should be preserved"
            calendar_table = G_copy.get_table("calendar")
            assert "FULLW" in calendar_table['service_id'].values, "FULLW service_id should be preserved"
            # stop_times
            stop_times_table = G_copy.get_table("stop_times")
            # 01:23:45 corresponds to 3600 + (32 * 60) + 45 [in day seconds]
            assert 3600 + (32 * 60) + 45 not in stop_times_table['arr_time']
            os.remove(fname_copy)

            # untested tables with filtering: stops, stops_rtree (non-crucial though), shapes
            # (Shapes are not provided in the test data currently)

            # test filtering by start and end time, copy full range
            filter_extract(G, fname_copy, start_date="2007-01-01", end_date="2011-01-01", update_metadata=False)
            hash_copy = hashlib.md5(open(fname_copy, 'rb').read()).hexdigest()
            assert hash_orig == hash_copy

            G_copy = GTFS(fname_copy)
            dsut_end = G_copy.get_day_start_ut("2010-12-31")
            dsut_to_trip_I = G_copy.get_tripIs_within_range_by_dsut(dsut_end, dsut_end + 24 * 3600)
            assert len(dsut_to_trip_I) > 0
            os.remove(fname_copy)

            # the end date is not included:
            filter_extract(G, fname_copy, start_date="2007-01-02", end_date="2010-12-31")
            hash_copy = hashlib.md5(open(fname_copy, 'rb').read()).hexdigest()
            assert hash_orig != hash_copy
            G_copy = GTFS(fname_copy)
            dsut_end = G_copy.get_day_start_ut("2010-12-31")
            dsut_to_trip_I = G_copy.get_tripIs_within_range_by_dsut(dsut_end, dsut_end + 24 * 3600)
            assert len(dsut_to_trip_I) == 0
            calendar_copy = G_copy.get_table("calendar")
            max_date_calendar = max([datetime.datetime.strptime(el, "%Y-%m-%d")
                                     for el in calendar_copy["end_date"].values])
            min_date_calendar = max([datetime.datetime.strptime(el, "%Y-%m-%d")
                                     for el in calendar_copy["start_date"].values])
            end_date_not_included = datetime.datetime.strptime("2010-12-31", "%Y-%m-%d")
            start_date_not_included = datetime.datetime.strptime("2007-01-01", "%Y-%m-%d")
            assert max_date_calendar < end_date_not_included, "the last date should not be included in calendar"
            assert start_date_not_included < min_date_calendar
            os.remove(fname_copy)

            # test that the db is split by a given spatial boundary
            filter_extract(G, fname_copy, buffer_lat=36.914893, buffer_lon=-116.76821, buffer_distance=50)
            G_copy = GTFS(fname_copy)

            stops_table = G_copy.get_table("stops")
            assert "FUR_CREEK_RES" not in stops_table['stop_id'].values
            assert "AMV" in stops_table['stop_id'].values
            assert len(stops_table['stop_id'].values) == 8

            import pandas
            conn_copy = sqlite3.connect(fname_copy)
            stop_ids_df = pandas.read_sql('SELECT stop_id from stop_times '
                                          'left join stops '
                                          'on stops.stop_I = stop_times.stop_I', conn_copy)
            stop_ids = stop_ids_df["stop_id"].values
            # print stop_ids
            assert "FUR_CREEK_RES" not in stop_ids
            assert "AMV" in stop_ids

            trips_table = G_copy.get_table("trips")
            # print trips_table
            assert "BFC1" not in trips_table['trip_id'].values

            routes_table = G_copy.get_table("routes")
            assert "BFC" not in routes_table['route_id'].values
            # cases:
            # whole trip excluded
            # whole route excluded
            # whole agency excluded
            # part of trip excluded
            # part of route excluded
            # part of agency excluded
            # not removing stops from a trip that returns into area
        finally:
            for fn in [fname, fname_copy]:
                if os.path.exists(fn) and os.path.isfile(fn):
                    os.remove(fn)