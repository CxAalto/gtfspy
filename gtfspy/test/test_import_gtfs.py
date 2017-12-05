from __future__ import print_function

import os
import sqlite3
import unittest

from gtfspy.gtfs import GTFS
from gtfspy.import_gtfs import import_gtfs


# noinspection PyTypeChecker
class TestImport(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        """This method is run once for each class before any tests are run"""
        pass

    @classmethod
    def teardown_class(cls):
        """This method is run once for each class _after_ all tests are run"""
        pass

    def tearDown(self):
        # disconnect from the existing in memory connection
        self.conn.close()

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.conn = sqlite3.connect(':memory:')
        self.agencyText = \
            'agency_id, agency_name, agency_timezone, agency_url' \
            '\n ag1, CompNet, Europe/Zurich, www.example.com'
        self.stopsText = \
            'stop_id, stop_name, stop_lat, stop_lon, parent_station' \
            '\nSID1, "Parent-Stop-Name", 1.0, 2.0, ' \
            '\nSID2, Boring Stop Name, 1.1, 2.1, SID1' \
            '\n3, Boring Stop Name1, 1.2, 2.2, ' \
            '\n4, Boring Stop Name2, 1.3, 2.3, 3' \
            '\n5, StopCloseToFancyStop, 1.0001, 2.0001, ' \
            '\nT1, t1, 1.0001, 2.2, ' \
            '\nT2, t2, 1.0002, 2.2, ' \
            '\nT3, t3, 1.00015, 2.2, ' \
            '\nT4, t4, 1.0001, 2.2, '
        self.calendarText = \
            'service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday,' \
            'start_date, end_date' \
            '\nservice1, 1, 1, 1, 1, 1, 1, 1, 20160321, 20160327' \
            '\nservice2, 0, 0, 0, 0, 0, 0, 0, 20160321, 20160327' \
            '\nfreq_service, 1, 1, 1, 1, 1, 1, 1, 20160329, 20160329'
        self.calendarDatesText = \
            'service_id, date, exception_type' \
            '\nservice1, 20160322, 2' \
            '\nextra_service, 20160321, 1' \
            '\nservice2, 20160322, 1' \
            '\nphantom_service, 20160320, 2'
        # 1 -> service added
        # 2 -> service removed
        # note some same service IDs as in self.calendarText
        self.tripText = \
            "route_id, service_id, trip_id, trip_headsign, trip_short_name, shape_id" \
            "\nservice1_route, service1, service1_trip1, going north, trip_s1t1, shape_s1t1" \
            "\nservice2_route, service2, service2_trip1, going north, trip_s2t1, shape_s2t1" \
            "\nes_route, extra_service, es_trip1, going north, trip_es1, shape_es1" \
            "\nfrequency_route, freq_service, freq_trip_scheduled, going north, freq_name, shape_es1"
        self.routesText = \
            "route_id, agency_id, route_short_name, route_long_name, route_type" \
            "\nservice1_route, ag1, r1, route1, 0" \
            "\nservice2_route, ag1, r2, route2, 1" \
            "\nfrequency_route, ag1, freq_route, schedule frequency route, 2"
        self.shapeText = \
            "shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence" \
            "\n shape_s1t1,1.0,2.0,0" \
            "\n shape_s1t1,1.001,2.0,1" \
            "\n shape_s1t1,1.001,2.001,10" \
            "\n shape_s1t1,1.10001,2.10001,100"
        self.stopTimesText = \
            "trip_id, arrival_time, departure_time, stop_sequence, stop_id" \
            "\nservice1_trip1,0:06:10,0:06:10,0,SID1" \
            "\nservice1_trip1,0:06:15,0:06:16,1,SID2" \
            "\nfreq_trip_scheduled,0:00:00,0:00:00,1,SID1" \
            "\nfreq_trip_scheduled,0:02:00,0:02:00,1,SID2"
        self.frequenciesText = \
            "trip_id, start_time, end_time, headway_secs, exact_times" \
            "\nfreq_trip_scheduled, 14:00:00, 16:00:00, 600, 1"
        self.transfersText = \
            "from_stop_id, to_stop_id, transfer_type, min_transfer_time" \
            "\nT1, T2, 0, " \
            "\nT2, T3, 1, " \
            "\nT3, T1, 2, 120" \
            "\nT1, T4, 3, "
        self.feedInfoText = \
            "feed_publisher_name, feed_publisher_url, feed_lang, feed_start_date, feed_end_date, feed_version" \
            "\nThePublisher, www.example.com, en, 20160321, 20160327, 1.0"

        self.fdict = {
            'agency.txt':           self.agencyText,
            'stops.txt':            self.stopsText,
            'calendar.txt':         self.calendarText,
            'calendar_dates.txt':   self.calendarDatesText,
            'trips.txt':            self.tripText,
            'routes.txt':           self.routesText,
            'shapes.txt':           self.shapeText,
            'stop_times.txt':       self.stopTimesText,
            'frequencies.txt':      self.frequenciesText,
            'transfers.txt':        self.transfersText,
            'feed_info.txt':        self.feedInfoText
        }
        self.orig_row_factory = self.conn.row_factory

    def setDictConn(self):
        prev_factory = self.conn.row_factory

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        self.conn.row_factory = dict_factory
        return prev_factory

    def setRowConn(self):
        prev_factory = self.conn.row_factory
        self.conn.row_factory = self.orig_row_factory
        return prev_factory

    def printTable(self, table_name):
        """
        Pretty prints a table with name table_name.

        Parameters
        ----------
        table_name : str
            name of the table
        """
        prev_row_factory = self.setRowConn()
        print("")
        print("table " + table_name)
        print("-------------------")
        cur = self.conn.execute("SELECT * FROM %s" % table_name)
        names = [d[0] for d in cur.description]
        for name in names:
            print(name + ', ', end="")
        print("")
        for row in cur:
            print(row)
        self.conn.row_factory = prev_row_factory

    def tearDown(self):
        """This method is run once after _each_ test method is executed"""
        pass

    # def test_download_date_importing(self):
    #     path = tempfile.mkdtemp(prefix='2015-02-03', dir="/tmp/2016-02-03/")
    #     print path
    #     tempfile.mkstemp(suffix='.sqlite', prefix='tmp', dir="/tmp/2015-02-03/")

    def test_stopLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        self.setDictConn()
        # sqlite returns now list of dicts
        rows = self.conn.execute("SELECT * FROM stops").fetchall()
        assert len(rows) > 4  # some data should be imported
        assert rows[0]['stop_I'] == 1
        # Store quotes in names:
        parent_index = None
        for i, row in enumerate(rows):
            if row['name'] == '"Parent-Stop-Name"':
                parent_index = i
                break
        assert parent_index is not None
        parent_stop_I = rows[parent_index]['stop_I']
        boring_index = None
        for i, row in enumerate(rows):
            if row['name'] == "Boring Stop Name":
                boring_index = i
                break
        assert boring_index is not None
        assert rows[boring_index]['parent_I'] == parent_stop_I
        assert rows[boring_index]['self_or_parent_I'] == parent_stop_I
        assert rows[3]['self_or_parent_I'] == 3

    def test_agencyLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        self.conn.commit()
        cursor = self.conn.cursor()
        rows = cursor.execute("SELECT agency_id FROM agencies").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == u'ag1', rows[0][0]

    def test_agencyLoaderTwoTimeZonesFail(self):
        newagencytext = \
            self.agencyText + "\n123, AgencyFromDifferentTZ, Europe/Helsinki, www.buahaha.com"
        self.fdict['agency.txt'] = newagencytext
        with self.assertRaises(ValueError):
            import_gtfs(self.fdict, self.conn, preserve_connection=True)

    def test_routeLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        assert len(self.conn.execute("SELECT * FROM routes").fetchall()) > 0

    def test_calendarLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        self.setDictConn()
        rows = self.conn.execute("SELECT * FROM calendar").fetchall()
        assert len(rows[0]) == 11
        for key in 'm t w th f s su start_date end_date service_id service_I'.split():
            assert key in rows[0], 'no key ' + key

    def test_calendarDatesLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        rows = self.conn.execute("SELECT * FROM calendar").fetchall()
        self.setDictConn()
        rows = self.conn.execute("SELECT * FROM calendar_dates").fetchall()
        for row in rows:
            assert isinstance(row['service_I'], int)
        # calendar table should be increased by two dummy row
        rows = self.conn.execute("SELECT * "
                                 "FROM calendar "
                                 "WHERE service_id='phantom_service'").fetchall()
        # Whether this should be the case is negotiable, though
        self.assertEqual(len(rows), 1, "phantom service should be present in the calendar")

    def test_tripLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        rows = self.conn.execute("SELECT * FROM trips").fetchall()
        self.assertGreaterEqual(len(rows), 1)

    def test_dayLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        # Now, there should be
        # a regular trip according to calendar dates without any exceptions:
        self.setDictConn()
        query1 = "SELECT trip_I " \
                 "FROM days " \
                 "JOIN trips " \
                 "USING(trip_I) " \
                 "JOIN calendar " \
                 "USING(service_I) " \
                 "WHERE date='2016-03-21'" \
                 "AND service_id='service1'"
        res = self.conn.execute(query1).fetchall()
        assert len(res) == 1
        trip_I_service_1 = res[0]['trip_I']
        print(trip_I_service_1)
        query2 = "SELECT * FROM days WHERE trip_I=%s" % trip_I_service_1
        self.assertEqual(len(self.conn.execute(query2).fetchall()), 6,
                         "There should be 6 days with the trip_I "
                         "corresponding to service_id service1")
        query3 = "SELECT * " \
                 "FROM days " \
                 "JOIN trips " \
                 "USING(trip_I) " \
                 "JOIN calendar " \
                 "USING(service_I) " \
                 "WHERE date='2016-03-22'" \
                 "AND service_id='service1'"
        self.assertEqual(len(self.conn.execute(query3).fetchall()), 0,
                         "There should be no trip on date 2016-03-22"
                         "for service1 due to calendar_dates")
        query4 = "SELECT date " \
                 "FROM days " \
                 "JOIN trips " \
                 "USING(trip_I) " \
                 "JOIN calendar " \
                 "USING(service_I) " \
                 "WHERE service_id='service2'"
        self.assertEqual(len(self.conn.execute(query4).fetchall()), 1,
                         "There should be only one trip for service 2")
        self.assertEqual(self.conn.execute(query4).fetchone()['date'], "2016-03-22",
                         "and the date should be 2016-03-22")
        query6 = "SELECT * " \
                 "FROM days " \
                 "JOIN trips " \
                 "USING(trip_I) " \
                 "JOIN calendar " \
                 "USING(service_I) " \
                 "WHERE service_id='phantom_service'"
        res = self.conn.execute(query6).fetchall()

        self.assertEqual(len(res), 0, "there should be no phantom trips due to phantom service"
                                      "even though phantom service is in calendar"
                         )

    def test_shapeLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        self.setDictConn()
        keys = ['shape_id', 'lat', 'lon', 'seq', 'd']
        table = self.conn.execute("SELECT * FROM shapes").fetchall()
        assert table[1]['d'] > 0, "distance traveled should be > 0"
        for key in keys:
            assert key in table[0], "key " + key + " not in shapes table"

    def test_stopTimesLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        self.setDictConn()
        stoptimes = self.conn.execute("SELECT * FROM stop_times").fetchall()
        keys = ['stop_I', 'shape_break', 'trip_I', 'arr_time',
                'dep_time', 'seq', 'arr_time_ds', 'dep_time_ds']
        for key in keys:
            assert key in stoptimes[0]
        assert stoptimes[0]['dep_time_ds'] == 370
        assert stoptimes[0]['shape_break'] == 0
        assert stoptimes[1]['shape_break'] == 3

    def test_stopDistancesLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        query = "SELECT * FROM stop_distances"
        # set dictionary like row connection:
        self.setDictConn()
        rows = self.conn.execute(query).fetchall()
        assert len(rows) > 0
        for row in rows:
            print(row)
            assert row['d'] >= 0, "distance should be defined for all pairs in the stop_distances table"

    def test_metaDataLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        try:
            query = "SELECT * FROM metadata"
            self.conn.execute(query)
        except AssertionError:
            assert False, "The database should have a table named metadata"

    def test_frequencyLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        # "\nfrequency_route, freq_service, freq_trip, going north, freq_name, shape_es1" \
        keys = ["trip_I", "start_time", "end_time", "headway_secs", "exact_times", "start_time_ds", "end_time_ds"]
        self.setDictConn()
        rows = self.conn.execute("SELECT * FROM frequencies").fetchall()
        for key in keys:
            row = rows[0]
            assert key in row
        for row in rows:
            if row["start_time_ds"] == 14 * 3600:
                self.assertEqual(row["exact_times"], 1)
        # there should be twelve trips with service_I freq
        count = self.conn.execute("SELECT count(*) AS count FROM trips JOIN calendar "
                                  "USING(service_I) WHERE service_id='freq_service'").fetchone()['count']

        assert count == 12, count
        rows = self.conn.execute("SELECT trip_I FROM trips JOIN calendar "
                                 "USING(service_I) WHERE service_id='freq_service'").fetchall()
        for row in rows:
            trip_I = row['trip_I']
            res = self.conn.execute("SELECT * FROM stop_times WHERE trip_I={trip_I}".format(trip_I=trip_I)).fetchall()
            assert len(res) > 1, res
        self.setRowConn()
        g = GTFS(self.conn)
        print("Stop times: \n\n ", g.get_table("stop_times"))
        print("Frequencies: \n\n ", g.get_table("frequencies"))
        # should there be more tests?
        # check that the original trip_id does not exist in frequencies, trips, or stop_times?

    def test_transfersLoader(self):
        """
        First tests that the basic import to the transfers table is correct, and then checks that
        the information from transfers.txt is also flows to the stop_distances table.
        """
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        keys = ["from_stop_I", "to_stop_I", "transfer_type", "min_transfer_time"]
        self.setDictConn()
        transfers = self.conn.execute("SELECT * FROM transfers").fetchall()
        for key in keys:
            transfer = transfers[0]
            assert key in transfer

        from_stop_I_no_transfer = None
        to_stop_I_no_transfer = None
        from_stop_I_timed_transfer = None
        to_stop_I_timed_transfer = None
        from_stop_I_min_transfer = None
        to_stop_I_min_transfer = None
        min_transfer_time_min_transfer = None

        for transfer in transfers:
            transfer_type = transfer["transfer_type"]
            from_stop_I = transfer['from_stop_I']
            to_stop_I = transfer['to_stop_I']
            min_transfer_time = transfer["min_transfer_time"]
            assert isinstance(from_stop_I, int)
            assert isinstance(to_stop_I, int)
            assert isinstance(transfer_type, int)
            assert isinstance(min_transfer_time, int) or (min_transfer_time is None)
            if transfer["transfer_type"] == 3:  # no transfer
                from_stop_I_no_transfer = from_stop_I
                to_stop_I_no_transfer = to_stop_I
            elif transfer["transfer_type"] == 2:
                from_stop_I_min_transfer = from_stop_I
                to_stop_I_min_transfer = to_stop_I
                min_transfer_time_min_transfer = min_transfer_time
            elif transfer["transfer_type"] == 1:
                from_stop_I_timed_transfer = from_stop_I
                to_stop_I_timed_transfer = to_stop_I

        base_query = "SELECT * FROM stop_distances WHERE from_stop_I=? and to_stop_I=?"
        # no_transfer
        no_transfer_rows = self.conn.execute(base_query, (from_stop_I_no_transfer, to_stop_I_no_transfer)).fetchall()
        assert len(no_transfer_rows) == 0
        timed_transfer_rows = \
            self.conn.execute(base_query, (from_stop_I_timed_transfer, to_stop_I_timed_transfer)).fetchall()
        assert len(timed_transfer_rows) == 1
        assert timed_transfer_rows[0]['min_transfer_time'] == 0
        min_transfer_rows = \
            self.conn.execute(base_query, (from_stop_I_min_transfer, to_stop_I_min_transfer)).fetchall()
        assert len(min_transfer_rows) == 1
        assert min_transfer_rows[0]['min_transfer_time'] == min_transfer_time_min_transfer

    def test_feedInfoLoader(self):
        import_gtfs(self.fdict, self.conn, preserve_connection=True)
        keys = ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date"]
        self.setDictConn()
        rows = self.conn.execute("SELECT * FROM feed_info").fetchall()
        for key in keys:
            row = rows[0]
            assert key in row

    def test_testDataImport(self):
        gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        import_gtfs(gtfs_source_dir, self.conn, preserve_connection=True)

    def test_zipImport(self):
        gtfs_source_zip = os.path.join(os.path.dirname(__file__), "test_data/test_gtfs.zip")
        import_gtfs(gtfs_source_zip, self.conn, preserve_connection=True)

    def test_importMultiple(self):
        gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")

        gtfs_sources = [gtfs_source_dir, self.fdict]
        error_raised = False
        try:
            import_gtfs(gtfs_sources, self.conn, preserve_connection=True)
        except ValueError:
            error_raised = True
        assert error_raised, "different timezones in multiple feeds should raise an error"


        #mod_agencyText = \
        #    'agency_id, agency_name, agency_timezone, agency_url' \
         #   '\nag1, CompNet, America/Los_Angeles, www.example.com'
        #self.fdict['agency.txt'] = mod_agencyText


        # test that if trip_id:s (or stop_id:s etc. ) are the same in two feeds,
        # they get different trip_Is in the database created

        self.tearDown()
        self.setUp()

        # assert if importing two of the same feed will create the double number of trips
        gtfs_source = [self.fdict]
        import_gtfs(gtfs_source, self.conn, preserve_connection=True)
        n_rows_ref = self.conn.execute("SELECT count(*) FROM trips").fetchone()[0]
        self.tearDown()
        self.setUp()
        gtfs_sources = [self.fdict, self.fdict]
        import_gtfs(gtfs_sources, self.conn, preserve_connection=True)
        n_rows_double = self.conn.execute("SELECT count(*) FROM trips").fetchone()[0]
        self.assertEqual(n_rows_double, 2*n_rows_ref)

        # check for duplicate trip_I's
        rows = self.conn.execute("SELECT count(*) FROM trips GROUP BY trip_I").fetchall()
        for row in rows:
            self.assertIs(row[0],1)

        # check for duplicate service_I's in calendar
        rows = self.conn.execute("SELECT count(*) FROM calendar GROUP BY service_I").fetchall()
        for row in rows:
            self.assertIs(row[0], 1)

        # check for duplicate service_I's in calendar_dates
        rows = self.conn.execute("SELECT count(*) FROM calendar_dates GROUP BY service_I").fetchall()
        for row in rows:
            self.assertIs(row[0], 1)

        # check for duplicate route_I's
        rows = self.conn.execute("SELECT count(*) FROM routes GROUP BY route_I").fetchall()
        for row in rows:
            self.assertIs(row[0], 1)

        # check for duplicate agency_I's
        rows = self.conn.execute("SELECT count(*) FROM agencies GROUP BY agency_I").fetchall()
        for row in rows:
            self.assertIs(row[0], 1)

        # check for duplicate stop_I's
        rows = self.conn.execute("SELECT count(*) FROM stops GROUP BY stop_I").fetchall()
        for row in rows:
            self.assertIs(row[0], 1)

    def test_sources_required(self):
        self.fdict.pop("stops.txt")
        with self.assertRaises(AssertionError):
            import_gtfs(self.fdict, self.conn)

    def test_sources_required_multiple(self):
        fdict_copy = dict(self.fdict)
        fdict_copy.pop("stops.txt")
        with self.assertRaises(AssertionError):
            import_gtfs([self.fdict, fdict_copy], self.conn)

    def test_resequencing_stop_times(self):
        gtfs_source = self.fdict.copy()
        gtfs_source.pop('stop_times.txt')

        gtfs_source['stop_times.txt'] = \
            self.stopTimesText = \
            "trip_id, arrival_time, departure_time, stop_sequence, stop_id" \
            "\nservice1_trip1,0:06:10,0:06:10,0,SID1" \
            "\nservice1_trip1,0:06:15,0:06:16,10,SID2" \
            "\nfreq_trip_scheduled,0:00:00,0:00:00,1,SID1" \
            "\nfreq_trip_scheduled,0:02:00,0:02:00,123,SID2"
        import_gtfs(gtfs_source, self.conn, preserve_connection=True)

        rows = self.conn.execute("SELECT seq FROM stop_times ORDER BY trip_I, seq").fetchall()
        for row in rows:
            print(row)
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 2)
        self.assertEqual(rows[2][0], 1)
        self.assertEqual(rows[3][0], 2)

    def test_metaData(self):
        # TODO! untested
        pass
