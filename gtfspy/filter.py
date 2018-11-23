import time
import os
import shutil
import logging
import sqlite3
import datetime

import pandas

import gtfspy
from gtfspy import util
from gtfspy.gtfs import GTFS
from gtfspy.import_loaders.day_loader import recreate_days_table
from gtfspy.import_loaders.day_trips_materializer import recreate_day_trips2_table
from gtfspy.import_loaders.stop_times_loader import resequence_stop_times_seq_values
from gtfspy.import_loaders.trip_loader import update_trip_travel_times_ds
from gtfspy.util import wgs84_distance, set_process_timezone
from gtfspy import stats
from gtfspy import gtfs

FILTERED = True
NOT_FILTERED = False

DELETE_FREQUENCIES_NOT_REFERENCED_IN_TRIPS_SQL = \
    "DELETE FROM frequencies WHERE trip_I NOT IN (SELECT DISTINCT trip_I FROM trips)"
DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL = \
    'DELETE FROM shapes WHERE shape_id NOT IN (SELECT shape_id FROM trips)'
DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL = \
    'DELETE FROM routes WHERE route_I NOT IN (SELECT route_I FROM trips)'
DELETE_DAYS_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL = \
    "DELETE FROM days WHERE trip_I NOT IN (SELECT trip_I FROM trips)"
DELETE_DAY_TRIPS2_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL = \
    "DELETE FROM day_trips2 WHERE trip_I NOT IN (SELECT trip_I FROM trips)"
DELETE_FREQUENCIES_ENTRIES_NOT_PRESENT_IN_TRIPS = \
    "DELETE FROM frequencies WHERE trip_I NOT IN (SELECT trip_I FROM trips)"
DELETE_CALENDAR_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL = \
    "DELETE FROM calendar WHERE service_I NOT IN (SELECT distinct(service_I) FROM trips)"
DELETE_CALENDAR_DATES_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL = \
    "DELETE FROM calendar_dates WHERE service_I NOT IN (SELECT distinct(service_I) FROM trips)"
DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL = \
    "DELETE FROM agencies WHERE agency_I NOT IN (SELECT distinct(agency_I) FROM routes)"
DELETE_STOP_TIMES_NOT_REFERENCED_IN_TRIPS_SQL = \
    'DELETE FROM stop_times WHERE trip_I NOT IN (SELECT trip_I FROM trips)'
DELETE_STOP_DISTANCE_ENTRIES_WITH_NONEXISTENT_STOPS_SQL = \
    "DELETE FROM stop_distances " \
    "WHERE from_stop_I NOT IN (SELECT stop_I FROM stops) " \
    " OR to_stop_I NOT IN (SELECT stop_I FROM stops)"
DELETE_TRIPS_NOT_IN_DAYS_SQL = \
    'DELETE FROM trips WHERE trip_I NOT IN (SELECT trip_I FROM days)'
DELETE_TRIPS_NOT_REFERENCED_IN_STOP_TIMES = \
    'DELETE FROM trips WHERE trip_I NOT IN (SELECT trip_I FROM stop_times)'


class FilterExtract(object):

    def __init__(self,
                 G,
                 copy_db_path,
                 buffer_distance_km=None,
                 buffer_lat=None,
                 buffer_lon=None,
                 stops_to_keep=None,
                 update_metadata=True,
                 start_date=None,
                 end_date=None,
                 trip_earliest_start_time_ut=None,
                 trip_latest_start_time_ut=None,
                 agency_ids_to_preserve=None,
                 agency_distance=None,
                 split_trips_partially_outside_buffer=True):
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
        start_date : str, or datetime.datetime
            filter out all data taking place before end_date (the start_time_ut of the end date)
            Date format "YYYY-MM-DD"
            (end_date_ut is not included after filtering)
        end_date : str, or datetime.datetime
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

        Returns
        -------
        None
        """
        self.gtfs = G
        tz = self.gtfs.get_timezone_pytz()
        self.start_time_ut = trip_earliest_start_time_ut
        self.end_time_ut = trip_latest_start_time_ut
        if self.start_time_ut and self.end_time_ut:
            start_date = util.ut_to_utc_datetime(self.start_time_ut, tz)
            end_date = util.ut_to_utc_datetime(self.end_time_ut, tz)
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

        self.agency_ids_to_preserve = agency_ids_to_preserve
        self.split_trips_partially_outside_buffer = split_trips_partially_outside_buffer
        self.buffer_lat = buffer_lat
        self.buffer_lon = buffer_lon
        self.buffer_distance_km = buffer_distance_km
        self.stops_to_keep = stops_to_keep
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

            filtered = False
            filtered = self._delete_rows_by_start_and_end_date() or filtered
            if self.copy_db_conn.execute('SELECT count(*) FROM days').fetchone() == (0,):
                raise ValueError('No data left after filtering')
            filtered = self._filter_by_calendar() or filtered
            filtered = self._filter_by_agency() or filtered
            filtered = self._filter_spatially() or filtered
            self.copy_db_conn.commit()
            if filtered:
                update_secondary_data_copies(db_conn=self.copy_db_conn)
            if self.update_metadata:
                self._update_metadata()
        return

    def _delete_rows_by_start_and_end_date(self):
        """
        Removes rows from the sqlite database copy that are out of the time span defined by start_date and end_date.
        """
        # filter by start_time_ut and end_date_ut:
        start_date_ut = None
        end_date_ut = None
        if self.start_time_ut and self.end_time_ut:
            start_date_ut = self.start_time_ut
            end_date_ut = self.end_time_ut
        if (self.start_date is not None) and (self.end_date is not None):
            start_date_ut = self.gtfs.get_day_start_ut(self.start_date)
            end_date_ut = self.gtfs.get_day_start_ut(self.end_date)

        if end_date_ut and start_date_ut:
            if self.copy_db_conn.execute("SELECT count(*) FROM day_trips2 WHERE start_time_ut IS null "
                                         "OR end_time_ut IS null").fetchone() != (0,):
                raise ValueError("Missing information in day_trips2 (start_time_ut and/or end_time_ut), "
                                 "check trips.start_time_ds and trips.end_time_ds.")
            logging.info("Filtering based on start_time_ut and end_time_ut")
            table_to_preserve_map = {
                "calendar": "start_date < date({filter_end_ut}, 'unixepoch', 'localtime') "
                            "AND "
                            "end_date >= date({filter_start_ut}, 'unixepoch', 'localtime') ",
                "calendar_dates": "date >= date({filter_start_ut}, 'unixepoch', 'localtime') "
                                  "AND "
                                  "date < date({filter_end_ut}, 'unixepoch', 'localtime') ",
                "day_trips2": 'start_time_ut < {filter_end_ut} '
                              'AND '
                              'end_time_ut > {filter_start_ut} ',
                "days": "day_start_ut >= {filter_start_ut} "
                        "AND "
                        "day_start_ut < {filter_end_ut} "
            }
            table_to_remove_map = \
                {key: "WHERE NOT ( " + to_preserve + " );" for key, to_preserve in table_to_preserve_map.items()}
            # Ensure that process timezone is correct as we rely on 'localtime' in the SQL statements.
            GTFS(self.copy_db_conn).set_current_process_time_zone()
            # remove the 'source' entries from tables
            for table, query_template in table_to_remove_map.items():
                param_dict = {"filter_start_ut": str(start_date_ut),
                              "filter_end_ut": str(end_date_ut)}
                query = "DELETE FROM " + table + " " + \
                        query_template.format(**param_dict)

                self.copy_db_conn.execute(query)
                self.copy_db_conn.commit()

            return FILTERED
        else:
            return NOT_FILTERED

    def _filter_by_calendar(self):
        """
        Remove or update the start and end dates of services in calendar table.
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
            delete_stops_not_in_stop_times_and_not_as_parent_stop(self.copy_db_conn, stops_to_keep=self.stops_to_keep)
            self.copy_db_conn.execute(DELETE_STOP_DISTANCE_ENTRIES_WITH_NONEXISTENT_STOPS_SQL)
            self.copy_db_conn.execute(DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL)
            self.copy_db_conn.execute(DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL)
            self.copy_db_conn.commit()
            return FILTERED
        else:
            return NOT_FILTERED

    def _filter_by_agency(self):
        """
        Filter by agency ids
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
            return FILTERED
        else:
            return NOT_FILTERED

    def _filter_spatially(self):
        """
        Filter the feed based on self.buffer_distance_km from self.buffer_lon and self.buffer_lat.

        1. First include all stops that are within self.buffer_distance_km from self.buffer_lon and self.buffer_lat.
        2. Then include all intermediate stops that are between any of the included stop pairs with some PT trip.
        3. Repeat step 2 until no more stops are to be included.

        As a summary this process should get rid of PT network tendrils, but should preserve the PT network intact
        at its core.
        """
        if any([self.buffer_lat is None, self.buffer_lon is None, self.buffer_distance_km is None]) \
                and self.stops_to_keep is None:
            return NOT_FILTERED

        if all([self.buffer_lat is not None, self.buffer_lon is not None, self.buffer_distance_km is not None]):
            print("filtering with lat: " + str(self.buffer_lat) +
                  " lon: " + str(self.buffer_lon) +
                  " buffer distance: " + str(self.buffer_distance_km))
            remove_all_trips_fully_outside_buffer(self.copy_db_conn,
                                                  self.buffer_lat,
                                                  self.buffer_lon,
                                                  self.buffer_distance_km,
                                                  update_secondary_data=False)
        elif self.stops_to_keep is not None:
            remove_all_trips_fully_outside_buffer(self.copy_db_conn,
                                                  stops_to_keep=self.stops_to_keep,
                                                  update_secondary_data=False)
            print("stops to preserve", len(self.stops_to_keep))
        logging.info("Making spatial extract")

        find_distance_func_name = add_wgs84_distance_function_to_db(self.copy_db_conn)
        assert find_distance_func_name == "find_distance"

        if not self.stops_to_keep:
            # select all stops that are within the buffer and have some stop_times assigned.
            stop_distance_filter_sql_base = (
                "SELECT DISTINCT stops.stop_I FROM stops, stop_times" +
                "    WHERE CAST(find_distance(lat, lon, {buffer_lat}, {buffer_lon}) AS INT) < {buffer_distance_meters}" +
                "     AND stops.stop_I=stop_times.stop_I"
            )
            stops_within_buffer_sql = stop_distance_filter_sql_base.format(
                buffer_lat=float(self.buffer_lat),
                buffer_lon=float(self.buffer_lon),
                buffer_distance_meters=int(self.buffer_distance_km * 1000)
            )
            stops_within_buffer = set(row[0] for row in self.copy_db_conn.execute(stops_within_buffer_sql))
        else:
            stops_within_buffer = self.stops_to_keep
        # For each trip_I, find smallest (min_seq) and largest (max_seq) stop sequence numbers that
        # are within the soft buffer_distance from the buffer_lon and buffer_lat, and add them into the
        # list of stops to preserve.
        # Note that if a trip is OUT-IN-OUT-IN-OUT, this process preserves (at least) the part IN-OUT-IN of the trip.
        # Repeat until no more stops are found.

        stops_within_buffer_string = "(" + ",".join(str(stop_I) for stop_I in stops_within_buffer) + ")"
        trip_min_max_include_seq_sql = (
            'SELECT trip_I, min(seq) AS min_seq, max(seq) AS max_seq FROM stop_times, stops '
                    'WHERE stop_times.stop_I = stops.stop_I '
                    ' AND stops.stop_I IN {stop_I_list}'
                    ' GROUP BY trip_I'
        ).format(stop_I_list=stops_within_buffer_string)
        trip_I_min_seq_max_seq_df = pandas.read_sql(trip_min_max_include_seq_sql, self.copy_db_conn)

        for trip_I_seq_row in trip_I_min_seq_max_seq_df.itertuples():
            trip_I = trip_I_seq_row.trip_I
            min_seq = trip_I_seq_row.min_seq
            max_seq = trip_I_seq_row.max_seq
            # DELETE FROM STOP_TIMES
            if min_seq == max_seq:
                # Only one entry in stop_times to be left, remove whole trip.
                self.copy_db_conn.execute("DELETE FROM stop_times WHERE trip_I={trip_I}".format(trip_I=trip_I))
                self.copy_db_conn.execute("DELETE FROM trips WHERE trip_i={trip_I}".format(trip_I=trip_I))
            else:
                # DELETE STOP_TIME ENTRIES BEFORE ENTERING AND AFTER DEPARTING THE BUFFER AREA
                DELETE_STOP_TIME_ENTRIES_SQL = \
                    "DELETE FROM stop_times WHERE trip_I={trip_I} AND (seq<{min_seq} OR seq>{max_seq})"\
                    .format(trip_I=trip_I, max_seq=max_seq, min_seq=min_seq)
                self.copy_db_conn.execute(DELETE_STOP_TIME_ENTRIES_SQL)

                STOPS_NOT_WITHIN_BUFFER__FOR_TRIP_SQL = \
                    "SELECT seq, stop_I IN {stops_within_hard_buffer} AS within " \
                    "FROM stop_times " \
                    "WHERE trip_I={trip_I} " \
                    "ORDER BY seq"\
                    .format(stops_within_hard_buffer=stops_within_buffer_string, trip_I=trip_I)
                stop_times_within_buffer_df = pandas.read_sql(STOPS_NOT_WITHIN_BUFFER__FOR_TRIP_SQL, self.copy_db_conn)
                if stop_times_within_buffer_df['within'].all():
                    continue
                elif self.split_trips_partially_outside_buffer:
                    _split_trip(self.copy_db_conn, trip_I, stop_times_within_buffer_df)
                else:
                    # just delete the stop_times, without splitting trip
                    self.copy_db_conn.execute("DELETE FROM stop_times WHERE stop_I NOT IN {stop_I_list}".format(
                        stop_I_list=stops_within_buffer_string))


        # Delete all shapes that are not fully within the buffer to avoid shapes going outside
        # the buffer area in a some cases.
        # This could probably be done in some more sophisticated way though (per trip)
        if self.buffer_lat and self.buffer_lon and self.buffer_distance_km:
            SHAPE_IDS_NOT_WITHIN_BUFFER_SQL = \
                "SELECT DISTINCT shape_id FROM SHAPES " \
                "WHERE CAST(find_distance(lat, lon, {buffer_lat}, {buffer_lon}) AS INT) > {buffer_distance_meters}" \
                .format(buffer_lat=self.buffer_lat,
                        buffer_lon=self.buffer_lon,
                        buffer_distance_meters=self.buffer_distance_km * 1000)
            DELETE_ALL_SHAPE_IDS_NOT_WITHIN_BUFFER_SQL = "DELETE FROM shapes WHERE shape_id IN (" \
                                                         + SHAPE_IDS_NOT_WITHIN_BUFFER_SQL + ")"
            self.copy_db_conn.execute(DELETE_ALL_SHAPE_IDS_NOT_WITHIN_BUFFER_SQL)
            SET_SHAPE_ID_TO_NULL_FOR_HARD_BUFFER_FILTERED_SHAPE_IDS = \
                "UPDATE trips SET shape_id=NULL WHERE trips.shape_id IN (" + SHAPE_IDS_NOT_WITHIN_BUFFER_SQL + ")"
            self.copy_db_conn.execute(SET_SHAPE_ID_TO_NULL_FOR_HARD_BUFFER_FILTERED_SHAPE_IDS)

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

        # Delete all stops not present in stop_times
        delete_stops_not_in_stop_times_and_not_as_parent_stop(self.copy_db_conn, stops_to_keep=self.stops_to_keep)
        # Consecutively delete all the rest remaining.
        self.copy_db_conn.execute(DELETE_TRIPS_NOT_REFERENCED_IN_STOP_TIMES)
        self.copy_db_conn.execute(DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL)
        self.copy_db_conn.execute(DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL)
        self.copy_db_conn.execute(DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL)
        self.copy_db_conn.execute(DELETE_STOP_DISTANCE_ENTRIES_WITH_NONEXISTENT_STOPS_SQL)
        self.copy_db_conn.execute(DELETE_FREQUENCIES_ENTRIES_NOT_PRESENT_IN_TRIPS)
        remove_dangling_shapes(self.copy_db_conn)
        self.copy_db_conn.commit()
        return FILTERED

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


def delete_stops_not_in_stop_times_and_not_as_parent_stop(conn, stops_to_keep=None):
    stops_to_keep_string = ""
    if stops_to_keep is not None:
        stops_to_keep_string = " AND stop_I NOT IN ({stops_to_retain})".format(stops_to_retain=",".join([str(x) for x in stops_to_keep]))

    _STOPS_REFERENCED_IN_STOP_TIMES_OR_AS_PARENT_STOP_I_SQL = \
        "SELECT DISTINCT stop_I FROM stop_times " \
        "UNION " \
        "SELECT DISTINCT parent_I as stop_I FROM stops WHERE parent_I IS NOT NULL"
    DELETE_STOPS_NOT_REFERENCED_IN_STOP_TIMES_AND_NOT_PARENT_STOP_SQL = \
        "DELETE FROM stops WHERE stop_I NOT IN (" + \
        _STOPS_REFERENCED_IN_STOP_TIMES_OR_AS_PARENT_STOP_I_SQL + ")" + stops_to_keep_string
    # TODO: make it so that stops in stops_to_keep are note deleted. also remember the stop_distances entries
    # It is possible that there is some "parent_I" recursion going on, and thus we
    # execute the same SQL query three times.
    conn.execute(DELETE_STOPS_NOT_REFERENCED_IN_STOP_TIMES_AND_NOT_PARENT_STOP_SQL)
    conn.execute(DELETE_STOPS_NOT_REFERENCED_IN_STOP_TIMES_AND_NOT_PARENT_STOP_SQL)
    conn.execute(DELETE_STOPS_NOT_REFERENCED_IN_STOP_TIMES_AND_NOT_PARENT_STOP_SQL)


def add_wgs84_distance_function_to_db(conn):
    function_name = "find_distance"
    conn.create_function(function_name, 4, wgs84_distance)
    return function_name


def remove_all_trips_fully_outside_buffer(db_conn, center_lat=None, center_lon=None, buffer_km=None,
                                          update_secondary_data=True, stops_to_keep=None):
    """
    Not used in the regular filter process for the time being.

    Parameters
    ----------
    db_conn: sqlite3.Connection
        connection to the GTFS object
    center_lat: float
    center_lon: float
    buffer_km: float
    update_secondary_data: bool, optional
        Whether or not to update secondary data in
        stop_times, days, and day_trips2 tables.
        True recommended, unless you know what you are doing.
    """
    if stops_to_keep is None:
        distance_function_str = add_wgs84_distance_function_to_db(db_conn)
        stops_within_buffer_query_sql = \
            "SELECT stop_I FROM stops WHERE CAST(" + distance_function_str + \
            "(lat, lon, {lat} , {lon}) AS INT) < {d_m}"\
            .format(lat=float(center_lat), lon=float(center_lon), d_m=int(1000 * buffer_km))
    else:
        stops_within_buffer_query_sql = ",".join(str(x) for x in stops_to_keep)

    select_all_trip_Is_where_stop_I_is_within_buffer_sql = \
        "SELECT distinct(trip_I) FROM stop_times WHERE stop_I IN (" + stops_within_buffer_query_sql + ")"
    trip_Is_to_remove_sql = \
        "SELECT trip_I FROM trips WHERE trip_I NOT IN ( " + select_all_trip_Is_where_stop_I_is_within_buffer_sql + ")"
    trip_Is_to_remove = pandas.read_sql(trip_Is_to_remove_sql, db_conn)["trip_I"].values
    trip_Is_to_remove_string = ",".join([str(trip_I) for trip_I in trip_Is_to_remove])
    remove_all_trips_fully_outside_buffer_sql = "DELETE FROM trips WHERE trip_I IN (" + trip_Is_to_remove_string + ")"
    remove_all_stop_times_where_trip_I_fully_outside_buffer_sql = \
        "DELETE FROM stop_times WHERE trip_I IN (" + trip_Is_to_remove_string  + ")"
    db_conn.execute(remove_all_trips_fully_outside_buffer_sql)
    db_conn.execute(remove_all_stop_times_where_trip_I_fully_outside_buffer_sql)
    delete_stops_not_in_stop_times_and_not_as_parent_stop(db_conn, stops_to_keep=stops_to_keep)
    db_conn.execute(DELETE_ROUTES_NOT_PRESENT_IN_TRIPS_SQL)
    db_conn.execute(DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL)
    db_conn.execute(DELETE_DAYS_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL)
    db_conn.execute(DELETE_DAY_TRIPS2_ENTRIES_NOT_PRESENT_IN_TRIPS_SQL)
    db_conn.execute(DELETE_CALENDAR_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL)
    db_conn.execute(DELETE_CALENDAR_DATES_ENTRIES_FOR_NON_REFERENCE_SERVICE_IS_SQL)
    db_conn.execute(DELETE_FREQUENCIES_ENTRIES_NOT_PRESENT_IN_TRIPS)
    db_conn.execute(DELETE_AGENCIES_NOT_REFERENCED_IN_ROUTES_SQL)
    if update_secondary_data:
        update_secondary_data_copies(db_conn)


def remove_dangling_shapes(db_conn):
    """
    Remove dangling entries from the shapes directory.

    Parameters
    ----------
    db_conn: sqlite3.Connection
        connection to the GTFS object
    """
    db_conn.execute(DELETE_SHAPES_NOT_REFERENCED_IN_TRIPS_SQL)
    SELECT_MIN_MAX_SHAPE_BREAKS_BY_TRIP_I_SQL = \
        "SELECT trips.trip_I, shape_id, min(shape_break) as min_shape_break, max(shape_break) as max_shape_break " \
        "FROM trips, stop_times " \
        "WHERE trips.trip_I=stop_times.trip_I " \
        "GROUP BY trips.trip_I"
    trip_min_max_shape_seqs = pandas.read_sql(SELECT_MIN_MAX_SHAPE_BREAKS_BY_TRIP_I_SQL, db_conn)

    rows = []
    for row in trip_min_max_shape_seqs.itertuples():
        shape_id, min_shape_break, max_shape_break = row.shape_id, row.min_shape_break, row.max_shape_break
        if min_shape_break is None or max_shape_break is None:
            min_shape_break = float('-inf')
            max_shape_break = float('-inf')
        rows.append((shape_id, min_shape_break, max_shape_break))
    DELETE_SQL_BASE = "DELETE FROM shapes WHERE shape_id=? AND (seq<? OR seq>?)"
    db_conn.executemany(DELETE_SQL_BASE, rows)
    remove_dangling_shapes_references(db_conn)


def remove_dangling_shapes_references(db_conn):
    remove_danging_shapes_references_sql = \
        "UPDATE trips SET shape_id=NULL WHERE trips.shape_id NOT IN (SELECT DISTINCT shape_id FROM shapes)"
    db_conn.execute(remove_danging_shapes_references_sql)


def _split_trip(copy_db_conn, orig_trip_I, stop_times_within_buffer_df):
    blocks = []
    next_block = []
    for row in stop_times_within_buffer_df.itertuples():
        if row.within:
            next_block.append(row.seq)
        else:
            if len(next_block) > 1:
                blocks.append(next_block)
            next_block = []
    if len(next_block) > 1:
        blocks.append(next_block)
    orig_trip_df = pandas.read_sql("SELECT * FROM trips WHERE trip_I={trip_I}".format(trip_I=orig_trip_I), copy_db_conn)
    orig_trip_dict = orig_trip_df.to_dict(orient="records")[0]
    for i, seq_block in enumerate(blocks):
        # create new trip for each block,
        # with mostly same trip information as the original
        trip_id_generated = orig_trip_dict['trip_id'] + "_splitted_part_" + str(i)
        insert_generated_trip_sql = \
            "INSERT INTO trips (trip_id, route_I, service_I, direction_id, " \
            "shape_id, headsign, start_time_ds, end_time_ds) " \
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        values = [trip_id_generated, orig_trip_dict['route_I'],
                  orig_trip_dict['service_I'], orig_trip_dict['direction_id'],
                  None, orig_trip_dict['headsign'], None, None]
        copy_db_conn.execute(insert_generated_trip_sql, values)
        block_trip_I = copy_db_conn.execute("SELECT trip_I from trips WHERE trips.trip_id=?",
                                            [trip_id_generated]).fetchone()[0]
        # alter the trip_I values in the stop_times table for
        seq_values_to_update_str = "(" + ",".join(str(seq) for seq in seq_block) + ")"
        stop_times_update_sql = \
            "UPDATE stop_times SET trip_I={trip_I_generated} WHERE trip_I={orig_trip_I} AND seq IN {seq_block}".format(
                trip_I_generated=block_trip_I,
                orig_trip_I=orig_trip_I,
                seq_block=seq_values_to_update_str
            )
        copy_db_conn.execute(stop_times_update_sql)

    copy_db_conn.execute("DELETE FROM trips WHERE trip_I={orig_trip_I}".format(orig_trip_I=orig_trip_I))
    copy_db_conn.execute("DELETE FROM stop_times WHERE trip_I={orig_trip_I}".format(orig_trip_I=orig_trip_I))
    copy_db_conn.execute("DELETE FROM shapes WHERE shape_id IN "
                         " (SELECT DISTINCT shapes.shape_id FROM shapes, trips "
                         "     WHERE trip_I={orig_trip_I} AND shapes.shape_id=trips.shape_id)"
                         .format(orig_trip_I=orig_trip_I))


def update_secondary_data_copies(db_conn):
    G = gtfspy.gtfs.GTFS(db_conn)
    G.set_current_process_time_zone()
    update_trip_travel_times_ds(db_conn)
    resequence_stop_times_seq_values(db_conn)
    recreate_days_table(db_conn)
    recreate_day_trips2_table(db_conn)
