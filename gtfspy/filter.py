import time
import os
import shutil
import logging
import sqlite3
import datetime

import pandas

from gtfspy import util
from gtfspy.util import wgs84_distance
from gtfspy import stats
from gtfspy import gtfs

DELETE_STOPS_NOT_REFERENCED_IN_STOP_TIMES_AND_NOT_PARENT_STOP_SQL = \
    'DELETE FROM stops ' \
    '  WHERE stop_I NOT IN (SELECT distinct(stop_I) FROM stop_times) ' \
    '  AND stop_I NOT IN (SELECT distinct(parent_I))'
DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL = 'DELETE FROM shapes WHERE shape_id NOT IN (SELECT shape_id FROM trips)'
DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL = 'DELETE FROM routes WHERE route_I NOT IN (SELECT route_I FROM trips)'
DELETE_DAYS_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL = "DELETE FROM days WHERE trip_I NOT IN (SELECT trip_I FROM trips)"
DELETE_DAY_TRIPS2_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL = "DELETE FROM day_trips2 WHERE trip_I NOT IN (SELECT trip_I FROM trips)"
DELETE_FREQUENCIES_ENTRIES_NOT_PRESENT_IN_TRIPS = "DELETE FROM frequencies WHERE trip_I NOT IN (SELECT trip_I FROM trips)"
DELETE_CALENDAR_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL = "DELETE FROM calendar WHERE service_I NOT IN (SELECT distinct(service_I) FROM trips)"
DELETE_CALENDAR_DATES_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL = "DELETE FROM calendar_dates WHERE service_I NOT IN (SELECT distinct(service_I) FROM trips)"
DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL = "DELETE FROM agencies WHERE agency_I NOT IN (SELECT distinct(agency_I) FROM routes)"
DELETE_STOP_TIMES_NOT_REFERENCED_IN_TRIPS_SQL = 'DELETE FROM stop_times WHERE trip_I NOT IN (SELECT trip_I FROM trips)'
DELETE_STOP_DISTANCE_ENTRIES_WITH_NONEXISTENT_STOPS_SQL = "DELETE FROM stop_distances " \
                                                          "WHERE from_stop_I NOT IN (SELECT stop_I FROM stops) " \
                                                          " OR to_stop_I NOT IN (SELECT stop_I FROM stops)"
DELETE_TRIPS_NOT_IN_DAYS_SQL = 'DELETE FROM trips WHERE trip_I NOT IN (SELECT trip_I FROM days)'
DELETE_TRIPS_NOT_REFERENCED_IN_STOP_TIMES = 'DELETE FROM trips WHERE trip_I NOT IN (SELECT trip_I FROM stop_times)'



class FilterExtract(object):

    def __init__(self,
                 G,
                 copy_db_path,
                 buffer_distance=None,
                 hard_buffer_distance=None,
                 buffer_lat=None,
                 buffer_lon=None,
                 update_metadata=True,
                 start_date=None,
                 end_date=None,
                 agency_ids_to_preserve=None,
                 agency_distance=None):
        """
        Copy a database, and then based on various filters.
        Only method `create_filtered_copy` is provided as we do not want to take the risk of
        losing the data stored in the original database.

        G: gtfspy.gtfs.GTFS
            the original database
        copy_db_path : str
            path to another database database
        update_metadata : boolean, optional
            whether to update metadata of the feed, defaulting to true
            (this option is mainly available for testing purposes)
        start_date : unicode, or datetime.datetime
            filter out all data taking place before end_date (the start_time_ut of the end date)
            Date format "YYYY-MM-DD"
            (end_date_ut is not included after filtering)
        end_date : unicode, or datetime.datetime
            Filter out all data taking place after end_date
            The end_date is not included after filtering.
        agency_ids_to_preserve : iterable
            List of agency_ids to retain (str) (e.g. 'HSL' for Helsinki)
            Only routes by the listed agencies are then considered
        agency_distance : float
            Only evaluated in combination with agency filter.
            Distance (in km) to the other near-by stops that should be included in addition to
            the ones defined by the agencies.
            All vehicle trips going through at least two such stops would then be included in the
            export. Note that this should not be a recursive thing.
            Or should it be? :)
        buffer_lat : float
            Latitude of the buffer zone center
        buffer_lon : float
            Longitude of the buffer zone center
        buffer_distance : float
            Distance from the buffer zone center (in kilometers)
        hard_buffer_distance: float, optional
            Take away all operations beyond this limit.

        Returns
        -------
        None
        """
        if start_date and end_date:
            if isinstance(start_date, (datetime.datetime, datetime.date)):
                self.start_date = start_date.strftime("%Y-%m-%d")
            else:
                self.start_date = start_date
            if isinstance(end_date, (datetime.datetime, datetime.date)):
                end_date_dt = end_date
                self.end_date = end_date.strftime("%Y-%m-%d")
            else:
                self.end_date = end_date
                end_date_dt = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")
            end_date_to_include = end_date_dt - datetime.timedelta(days=1)
            self.end_date_to_include_str = end_date_to_include.strftime("%Y-%m-%d")
        else:
            self.start_date = None
            self.end_date = None
        self.copy_db_conn = None
        self.copy_db_path = copy_db_path

        self.end_date = end_date

        self.agency_ids_to_preserve = agency_ids_to_preserve
        self.gtfs = G
        self.buffer_lat = buffer_lat
        self.buffer_lon = buffer_lon
        self.buffer_distance_km = buffer_distance
        self.hard_buffer_distance = hard_buffer_distance
        self.update_metadata = update_metadata

        if agency_distance is not None:
            raise NotImplementedError

        self.this_db_path = self.gtfs.get_main_database_path()
        assert os.path.exists(self.this_db_path), "Copying of in-memory databases is not supported"
        assert os.path.exists(os.path.dirname(os.path.abspath(copy_db_path))), \
            "the directory where the copied database will reside should exist beforehand"
        assert not os.path.exists(copy_db_path), "the resulting database exists already: %s" % copy_db_path

    def create_filtered_copy(self):
        # this with statement
        # is used to ensure that no corrupted/uncompleted files get created in case of problems
        with util.create_file(self.copy_db_path) as tempfile:
            logging.info("copying database")
            shutil.copy(self.this_db_path, tempfile)
            self.copy_db_conn = sqlite3.connect(tempfile)
            assert isinstance(self.copy_db_conn, sqlite3.Connection)

            self._delete_rows_by_start_and_end_date()
            if self.copy_db_conn.execute('SELECT count(*) FROM days').fetchone() == (0,):
                raise ValueError('No data left after filtering')
            self._filter_by_calendar()
            self._filter_by_agency()
            self._filter_spatially()
            if self.update_metadata:
                self._update_metadata()

        return

    def _delete_rows_by_start_and_end_date(self):
        """
        Removes rows from the sqlite database copy that are out of the time span defined by start_date and end_date
        :param gtfs: GTFS object
        :param copy_db_conn: sqlite database connection
        :param start_date:
        :param end_date:
        :return:
        """
        # filter by start_time_ut and end_date_ut:
        if (self.start_date is not None) and (self.end_date is not None):
            start_date_ut = self.gtfs.get_day_start_ut(self.start_date)
            end_date_ut = self.gtfs.get_day_start_ut(self.end_date)
            if self.copy_db_conn.execute("SELECT count(*) FROM day_trips2 WHERE start_time_ut IS null "
                                         "OR end_time_ut IS null").fetchone() != (0,):
                raise ValueError("Missing information in day_trips2 (start_time_ut and/or end_time_ut), "
                                 "check trips.start_time_ds and trips.end_time_ds.")
            logging.info("Filtering based on agency_ids")
            # negated from import_gtfs
            table_to_remove_map = {
                "calendar": ("WHERE NOT ("
                             "date({start_ut}, 'unixepoch', 'localtime') < end_date "
                             "AND "
                             "start_date < date({end_ut}, 'unixepoch', 'localtime')"
                             ");"),
                "calendar_dates": "WHERE NOT ("
                                  "date({start_ut}, 'unixepoch', 'localtime') <= date "
                                  "AND "
                                  "date < date({end_ut}, 'unixepoch', 'localtime')"
                                  ")",
                "day_trips2": 'WHERE NOT ('
                              '{start_ut} < end_time_ut '
                              'AND '
                              'start_time_ut < {end_ut}'
                              ')',
                "days": "WHERE NOT ("
                        "{start_ut} <= day_start_ut "
                        "AND "
                        "day_start_ut < {end_ut}"
                        ")"
            }
            # remove the 'source' entries from tables
            for table, query_template in table_to_remove_map.items():
                param_dict = {"start_ut": str(start_date_ut),
                              "end_ut": str(end_date_ut)}
                if True and table == "days":
                    query = "SELECT * FROM " + table + " " + \
                            query_template.format(**param_dict)

                    self.gtfs.execute_custom_query_pandas(query)

                query = "DELETE FROM " + table + " " + \
                        query_template.format(**param_dict)
                self.copy_db_conn.execute(query)
        return

    def _soft_filter_by_calendar(self):
        pass
        """
        # TODO: soft filtering, where the recursive deletion of rows depends on the initially removed rows, not on missing links between ID fields.
        The reason for doing this is to detect unreferenced rows that possibly should be included in the filtered extract.
        This gives a much more accurate perspective on the quality of the feed







        PSeudocode:
        select all trip_I not to be deleted based on dates
        select all routes not to be deleted based on trip_I
        delete all agencies where agency_I not in routes to be saved nor is unreferenced in routes
        delete all routes where route_I not in trips to be saved
        delete all

        delete = {'agency': 'DELETE FROM agencies WHERE NOT agency_I IN',
                  'routes': 'DELETE FROM routes WHERE NOT route_I IN',
                  'trips': 'DELETE FROM trips WHERE NOT trip_I IN',
                  'stops': 'DELETE FROM stops WHERE NOT stop_I IN',
                  'stop_times': 'DELETE FROM stop_times WHERE NOT trip_I IN',
                  'days': 'DELETE trip_I FROM days WHERE NOT ({start_ut} <= day_start_ut AND day_start_ut < {end_ut})'}
        select = {'routes': 'SELECT agency_I FROM routes WHERE route_I IN',
                  'trips': 'SELECT route_I FROM trips WHERE trip_I IN',
                  'stop_times': 'SELECT stop_I FROM stop_times WHERE trip_I IN',
                  'days': 'SELECT trip_I FROM days WHERE ({start_ut} <= day_start_ut AND day_start_ut < {end_ut})'}
        'SELECT agency_I FROM agencies LEFT JOIN routes WHERE routes.'
        query = delete['agency']+select['routes']+select['trips']+select['days']

        agency_query = 'DELETE FROM agencies WHERE NOT agency_I IN (SELECT agency_I FROM routes WHERE route_I IN (SELECT route_I FROM trips WHERE trip_I IN ())) AND '
        """
    def _filter_by_calendar(self):
        """
        update calendar table's services
        :param copy_db_conn:
        :param start_date:
        :param end_date:
        :return:
        """

        if (self.start_date is not None) and (self.end_date is not None):

            logging.info("Making date extract")

            start_date_query = "UPDATE calendar " \
                               "SET start_date='{start_date}' " \
                               "WHERE start_date<'{start_date}' ".format(start_date=self.start_date)
            self.copy_db_conn.execute(start_date_query)

            end_date_query = "UPDATE calendar " \
                             "SET end_date='{end_date_to_include}' " \
                             "WHERE end_date>'{end_date_to_include}' " \
                .format(end_date_to_include=self.end_date_to_include_str)
            self.copy_db_conn.execute(end_date_query)

            # then recursively delete further data:
            self.copy_db_conn.execute(DELETE_TRIPS_NOT_IN_DAYS_SQL)
            self.copy_db_conn.execute(DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL)
            self.copy_db_conn.execute(DELETE_STOP_TIMES_NOT_REFERENCED_IN_TRIPS_SQL)
            self.copy_db_conn.execute(DELETE_STOPS_NOT_REFERENCED_IN_STOP_TIMES_AND_NOT_PARENT_STOP_SQL)
            self.copy_db_conn.execute(DELETE_STOP_DISTANCE_ENTRIES_WITH_NONEXISTENT_STOPS_SQL)
            self.copy_db_conn.execute(DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL)
            self.copy_db_conn.execute(DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL)
            self.copy_db_conn.commit()
        return

    def _filter_by_agency(self):
        """
        filter by agency ids
        :param copy_db_conn:
        :param agency_ids_to_preserve:
        :return:
        """
        if self.agency_ids_to_preserve is not None:
            logging.info("Filtering based on agency_ids")
            agency_ids_to_preserve = list(self.agency_ids_to_preserve)
            agencies = pandas.read_sql("SELECT * FROM agencies", self.copy_db_conn)
            agencies_to_remove = []
            for idx, row in agencies.iterrows():
                if row['agency_id'] not in agency_ids_to_preserve:
                    agencies_to_remove.append(row['agency_id'])
            for agency_id in agencies_to_remove:
                self.copy_db_conn.execute('DELETE FROM agencies WHERE agency_id=?', (agency_id,))
            # and remove recursively related to the agencies:
            self.copy_db_conn.execute('DELETE FROM routes WHERE '
                                      'agency_I NOT IN (SELECT agency_I FROM agencies)')
            self.copy_db_conn.execute('DELETE FROM trips WHERE '
                                      'route_I NOT IN (SELECT route_I FROM routes)')
            self.copy_db_conn.execute('DELETE FROM calendar WHERE '
                                      'service_I NOT IN (SELECT service_I FROM trips)')
            self.copy_db_conn.execute('DELETE FROM calendar_dates WHERE '
                                      'service_I NOT IN (SELECT service_I FROM trips)')
            self.copy_db_conn.execute('DELETE FROM days WHERE '
                                      'trip_I NOT IN (SELECT trip_I FROM trips)')
            self.copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                      'trip_I NOT IN (SELECT trip_I FROM trips)')
            self.copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                      'trip_I NOT IN (SELECT trip_I FROM trips)')
            self.copy_db_conn.execute('DELETE FROM shapes WHERE '
                                      'shape_id NOT IN (SELECT shape_id FROM trips)')
            self.copy_db_conn.execute('DELETE FROM day_trips2 WHERE '
                                      'trip_I NOT IN (SELECT trip_I FROM trips)')
            self.copy_db_conn.commit()
        return

    def _filter_spatially(self):
        """
        Filter the feed based on self.buffer_distance_km from self.buffer_lon and self.buffer_lat.

        1. First include all stops that are within self.buffer_distance_km from self.buffer_lon and self.buffer_lat.
        2. Then include all intermediate stops that are between any of the included stop pairs with some PT trip.
        3. Repeat step 2 until no more stops are to be included.

        As a summary this process should get rid of PT network tendrils, but should preserve the PT network intact
        at its core.
        """
        if self.buffer_lat is None or self.buffer_lon is None or self.buffer_distance_km is None:
            return

        print("filtering with lat: " + str(self.buffer_lat) +
              " lon: " + str(self.buffer_lon) +
              " buffer distance: " + str(self.buffer_distance_km))
        logging.info("Making spatial extract")

        find_distance_func_name = add_wgs84_distance_function_to_db(self.copy_db_conn)
        assert find_distance_func_name == "find_distance"

        # select all stops that are within the buffer and have some stop_times assigned.
        all_stops_within_buffer_sql = (
            "SELECT DISTINCT stops.stop_I FROM stops, stop_times"
            "    WHERE CAST(find_distance(lat, lon, {buffer_lat}, {buffer_lon}) AS INT) < {buffer_distance_meters}"
            "     AND stops.stop_I=stop_times.stop_I"
            .format(buffer_lat=float(self.buffer_lat),
                    buffer_lon=float(self.buffer_lon),
                    buffer_distance_meters=int(self.buffer_distance_km * 1000))
        )
        stops_to_preserve = set(row[0] for row in self.copy_db_conn.execute(all_stops_within_buffer_sql))

        # For each trip_I, find smallest (min_seq) and largest (max_seq) stop sequence numbers that
        # are within the buffer_distance from the buffer_lon and buffer_lat, and add them into the
        # list of stops to preserve.
        # Note that if a trip is OUT-IN-OUT-IN-OUT, this process preserves (at least) the part IN-OUT-IN of the trip.
        # Repeat until no more stops are found.
        while True:
            stops_string_sql = "(" +",".join(str(stop_I) for stop_I in stops_to_preserve) +  ")"
            trip_min_max_include_seq_sql =  (
                '(SELECT trip_I, min(seq) AS min_seq, max(seq) AS max_seq FROM stop_times, stops '
                        'WHERE stop_times.stop_I = stops.stop_I '
                        ' AND stops.stop_I IN {stop_I_list}'
                        ' GROUP BY trip_I) trip_min_max_seq'
            ).format(stop_I_list=stops_string_sql)
            stops_to_preserve_sql = (
                "SELECT DISTINCT stops.stop_I FROM stop_times, stops, " + trip_min_max_include_seq_sql +
                    ' WHERE stop_times.stop_I = stops.stop_I '
                        'AND stop_times.trip_I = trip_min_max_seq.trip_I '
                        'AND seq >= trip_min_max_seq.min_seq '
                        'AND seq <= trip_min_max_seq.max_seq ')

            # store the old list of stops to stops_to_preserve_before
            stops_to_preserve_before = stops_to_preserve
            stops_to_preserve = set(list(row[0] for row in self.copy_db_conn.execute(stops_to_preserve_sql)))

            assert stops_to_preserve_before.issubset(stops_to_preserve)
            if len(stops_to_preserve) == len(stops_to_preserve_before):
                break # no more new stops found -> exit while loop

        stops_string_sql = "(" + ",".join(str(stop_I) for stop_I in stops_to_preserve) + ")"

        print("stops before filtering: ", self.copy_db_conn.execute("SELECT count(*) FROM stops").fetchone()[0])
        self.copy_db_conn.execute("DELETE FROM stops WHERE stop_I NOT IN " + stops_string_sql)
        print("stops after first filtering: ", self.copy_db_conn.execute("SELECT count(*) FROM stops").fetchone()[0])

        if self.hard_buffer_distance:
            print("filtering with hard buffer")
            _hard_buffer_distance = self.hard_buffer_distance * 1000
            self.copy_db_conn.execute('DELETE FROM stops '
                                      'WHERE stop_I NOT IN '
                                      '(SELECT stop_I FROM stops '
                                      'WHERE CAST(find_distance(lat, lon, ?, ?) AS INT) < ?) ',
                                      (self.buffer_lat, self.buffer_lon, _hard_buffer_distance))
            print("stops after hard_buffer filtering: ", self.copy_db_conn.execute("SELECT count(*) FROM stops").fetchone()[0])

        # Delete all stop_times for uncovered stops
        self.copy_db_conn.execute('DELETE FROM stop_times WHERE stop_I NOT IN (SELECT stop_I FROM stops)')
        # Delete trips with only one stop
        self.copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                  'trip_I IN (SELECT trip_I FROM '
                                  '(SELECT trip_I, count(*) AS N_stops from stop_times '
                                  'GROUP BY trip_I) q1 '
                                  'WHERE N_stops = 1)')

        # Delete trips with only one stop but several instances in stop_times
        self.copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                  'trip_I IN (SELECT q1.trip_I AS trip_I FROM '
                                    '(SELECT trip_I, stop_I, count(*) AS stops_per_stop FROM stop_times '
                                    'GROUP BY trip_I, stop_I) q1, '
                                    '(SELECT trip_I, count(*) as n_stops FROM stop_times '
                                    'GROUP BY trip_I) q2 '
                                    'WHERE q1.trip_I = q2.trip_I AND n_stops = stops_per_stop)')

        # Consecutively delete all the rest remaining.
        self.copy_db_conn.execute(DELETE_TRIPS_NOT_REFERENCED_IN_STOP_TIMES)
        self.copy_db_conn.execute(DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL)
        self.copy_db_conn.execute(DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL)
        self.copy_db_conn.execute(DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL)
        self.copy_db_conn.execute(DELETE_STOP_DISTANCE_ENTRIES_WITH_NONEXISTENT_STOPS_SQL)
        remove_dangling_shapes(self.copy_db_conn)
        self.copy_db_conn.commit()

    def _update_metadata(self):
        # Update metadata
        G_orig = self.gtfs
        if self.update_metadata:
            print("Updating metadata")
            logging.info("Updating metadata")
            G_copy = gtfs.GTFS(self.copy_db_conn)
            G_copy.meta['copied_from'] = self.this_db_path
            G_copy.meta['copy_time_ut'] = time.time()
            G_copy.meta['copy_time'] = time.ctime()

            # Copy some keys directly.
            try:
                for key in ['original_gtfs',
                            'download_date',
                            'location_name',
                            'timezone', ]:
                    G_copy.meta[key] = G_orig.meta[key]
            # This part is for gtfs objects with multiple sources
            except:
                for k, v in G_copy.meta.items():
                    if 'feed_' in k:
                        G_copy.meta[k] = G_orig.meta[k]
                for key in ['location_name',
                            'timezone', ]:
                    G_copy.meta[key] = G_orig.meta[key]
            # Update *all* original metadata under orig_ namespace.
            G_copy.meta.update(('orig_' + k, v) for k, v in G_orig.meta.items())

            stats.update_stats(G_copy)

            # print "Vacuuming..."
            self.copy_db_conn.execute('VACUUM;')
            # print "Analyzing..."
            self.copy_db_conn.execute('ANALYZE;')
            self.copy_db_conn.commit()
        return

def add_wgs84_distance_function_to_db(conn):
    function_name = "find_distance"
    conn.create_function(function_name, 4, wgs84_distance)
    return function_name


def remove_all_trips_fully_outside_buffer(db_conn, center_lat, center_lon, buffer_km):
    """
    Not used in the regular filter process for the time being.

    Parameters
    ----------
    db_conn: sqlite3.Connection
        connection to the GTFS object
    center_lat: float
    center_lon: float
    buffer_km: float
    """
    distance_function_str = add_wgs84_distance_function_to_db(db_conn)
    stops_within_buffer_query_sql = "SELECT stop_I FROM stops WHERE CAST(" + distance_function_str + \
                                "(lat, lon, {lat} , {lon}) AS INT) < {d_m}"\
        .format(lat=float(center_lat), lon=float(center_lon), d_m=int(1000*buffer_km))
    select_all_trip_Is_where_stop_I_is_within_buffer_sql = "SELECT distinct(trip_I) FROM stop_times WHERE stop_I IN (" + stops_within_buffer_query_sql + ")"
    trip_Is_to_remove_sql = "SELECT trip_I FROM trips WHERE trip_I NOT IN ( " + select_all_trip_Is_where_stop_I_is_within_buffer_sql + ")"
    remove_all_trips_fully_outside_buffer_sql = "DELETE FROM trips WHERE trip_I IN (" + trip_Is_to_remove_sql  + ")"
    remove_all_stop_times_where_trip_I_fully_outside_buffer_sql = "DELETE FROM stop_times WHERE trip_I IN (" + trip_Is_to_remove_sql  + ")"
    db_conn.execute(remove_all_trips_fully_outside_buffer_sql)
    db_conn.execute(remove_all_stop_times_where_trip_I_fully_outside_buffer_sql)
    db_conn.execute(DELETE_STOPS_NOT_REFERENCED_IN_STOP_TIMES_AND_NOT_PARENT_STOP_SQL)
    db_conn.execute(DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL)
    db_conn.execute(DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL)
    db_conn.execute(DELETE_DAYS_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL)
    db_conn.execute(DELETE_DAY_TRIPS2_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL)
    db_conn.execute(DELETE_CALENDAR_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL)
    db_conn.execute(DELETE_CALENDAR_DATES_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL)
    db_conn.execute(DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL)


def remove_dangling_shapes(db_conn):
    """
    Not used in the regular filter process for the time being.

    Parameters
    ----------
    db_conn: sqlite3.Connection
        connection to the GTFS object
    """
    db_conn.execute(DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL)
    SELECT_MIN_MAX_SHAPE_BREAKS_BY_TRIP_I_SQL = \
        "SELECT trips.trip_I, shape_id, min(shape_break) as min_shape_break, max(shape_break) as max_shape_break FROM trips, stop_times WHERE trips.trip_I=stop_times.trip_I GROUP BY trips.trip_I"
    trip_min_max_shape_seqs= pandas.read_sql(SELECT_MIN_MAX_SHAPE_BREAKS_BY_TRIP_I_SQL, db_conn)
    rows = [(row.shape_id, row.min_shape_break, row.max_shape_break) for row in trip_min_max_shape_seqs.itertuples()]
    DELETE_SQL_BASE = "DELETE FROM shapes WHERE shape_id=? AND (seq<? OR seq>?)"
    db_conn.executemany(DELETE_SQL_BASE, rows)





