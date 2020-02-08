import calendar
import datetime
import logging
import os
import sqlite3
import sys
import time
import warnings
from collections import Counter, defaultdict
from datetime import timedelta

import numpy
import pandas as pd
import pytz
from six import string_types

from gtfspy import shapes
from gtfspy.route_types import ALL_ROUTE_TYPES
from gtfspy.route_types import WALK
from gtfspy.util import wgs84_distance, wgs84_width, wgs84_height, set_process_timezone


class GTFS(object):
    def __init__(self, fname_or_conn):
        """Open a GTFS object

        Parameters
        ----------
        fname_or_conn: str | sqlite3.Connection
            path to the preprocessed gtfs database or a connection to a gtfs database
        """
        if isinstance(fname_or_conn, string_types):
            if os.path.isfile(fname_or_conn):
                self.conn = sqlite3.connect(fname_or_conn)
                self.fname = fname_or_conn
                # memory-mapped IO size, in bytes
                self.conn.execute("PRAGMA mmap_size = 1000000000;")
                # page cache size, in negative KiB.
                self.conn.execute("PRAGMA cache_size = -2000000;")
            else:
                raise FileNotFoundError("File " + fname_or_conn + " missing")
        elif isinstance(fname_or_conn, sqlite3.Connection):
            self.conn = fname_or_conn
            self._dont_close = True
        else:
            raise NotImplementedError(
                "Initiating GTFS using an object with type "
                + str(type(fname_or_conn))
                + " is not supported"
            )

        assert (
            self.conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchone()
            is not None
        )
        self.meta = GTFSMetadata(self.conn)

        # Bind functions
        self.conn.create_function("find_distance", 4, wgs84_distance)

        # Set timezones
        self._timezone = pytz.timezone(self.get_timezone_name())

    def __del__(self):
        if not getattr(self, "_dont_close", False) and hasattr(self, "conn"):
            self.conn.close()

    @classmethod
    def from_directory_as_inmemory_db(cls, gtfs_directory):
        """
        Instantiate a GTFS object by computing

        Parameters
        ----------
        gtfs_directory: str
            path to the directory for importing the database
        """
        # this import is here to avoid circular imports (which turned out to be a problem)
        from gtfspy.import_gtfs import import_gtfs

        conn = sqlite3.connect(":memory:")
        import_gtfs(gtfs_directory, conn, preserve_connection=True, print_progress=False)
        return cls(conn)

    def get_main_database_path(self):
        """
        Should return the path to the database

        Returns
        -------
        path : unicode
            path to the database, empty string for in-memory databases
        """
        cur = self.conn.cursor()
        cur.execute("PRAGMA database_list")
        rows = cur.fetchall()
        for row in rows:
            if row[1] == str("main"):
                return row[2]

    def get_location_name(self):
        return self.meta.get("location_name", "location_unknown")

    def get_shape_distance_between_stops(self, trip_I, from_stop_seq, to_stop_seq):
        """
        Get the distance along a shape between stops

        Parameters
        ----------
        trip_I : int
            trip_ID along which we travel
        from_stop_seq : int
            the sequence number of the 'origin' stop
        to_stop_seq : int
            the sequence number of the 'destination' stop

        Returns
        -------
        distance : float, None
            If the shape calculation succeeded, return a float, otherwise return None
            (i.e. in the case where the shapes table is empty)
        """

        query_template = "SELECT shape_break FROM stop_times WHERE trip_I={trip_I} AND seq={seq} "
        stop_seqs = [from_stop_seq, to_stop_seq]
        shape_breaks = []
        for seq in stop_seqs:
            q = query_template.format(seq=seq, trip_I=trip_I)
            shape_breaks.append(self.conn.execute(q).fetchone())
        query_template = (
            "SELECT max(d) - min(d) "
            "FROM shapes JOIN trips ON(trips.shape_id=shapes.shape_id) "
            "WHERE trip_I={trip_I} AND shapes.seq>={from_stop_seq} AND shapes.seq<={to_stop_seq};"
        )
        distance_query = query_template.format(
            trip_I=trip_I, from_stop_seq=from_stop_seq, to_stop_seq=to_stop_seq
        )
        return self.conn.execute(distance_query).fetchone()[0]

    def get_stop_distance(self, from_stop_I, to_stop_I):
        query_template = "SELECT d_walk FROM stop_distances WHERE from_stop_I={from_stop_I} AND to_stop_I={to_stop_I} "
        q = query_template.format(from_stop_I=int(from_stop_I), to_stop_I=int(to_stop_I))
        if self.conn.execute(q).fetchone():
            return self.conn.execute(q).fetchone()[0]
        else:
            return None

    def get_stops_within_distance(self, stop, distance):
        query = """SELECT stops.* FROM stop_distances, stops
                    WHERE stop_distances.to_stop_I = stops.stop_I
                    AND d < %s AND from_stop_I = %s""" % (
            distance,
            stop,
        )
        return pd.read_sql_query(query, self.conn)

    def get_directly_accessible_stops_within_distance(self, stop, distance):
        """
        Returns stops that are accessible without transfer from the stops that are within a specific walking distance
        :param stop: int
        :param distance: int
        :return:
        """
        query = """SELECT stop.* FROM
                    (SELECT st2.* FROM
                    (SELECT * FROM stop_distances
                    WHERE from_stop_I = %s) sd,
                    (SELECT * FROM stop_times) st1,
                    (SELECT * FROM stop_times) st2
                    WHERE sd.d < %s AND sd.to_stop_I = st1.stop_I AND st1.trip_I = st2.trip_I
                    GROUP BY st2.stop_I) sq,
                    (SELECT * FROM stops) stop
                    WHERE sq.stop_I = stop.stop_I""" % (
            stop,
            distance,
        )
        return pd.read_sql_query(query, self.conn)

    def get_cursor(self):
        """
        Return a cursor to the underlying sqlite3 object
        """
        return self.conn.cursor()

    def get_table(self, table_name):
        """
        Return a pandas.DataFrame object corresponding to the sql table

        Parameters
        ----------
        table_name: str
            name of the table in the database

        Returns
        -------
        df : pandas.DataFrame
        """
        return pd.read_sql("SELECT * FROM " + table_name, self.conn)

    def get_row_count(self, table):
        """
        Get number of rows in a table
        """
        return self.conn.cursor().execute("SELECT count(*) FROM " + table).fetchone()[0]

    def get_table_names(self):
        """
        Return a list of the underlying tables in the database.

        Returns
        -------
        table_names: list[str]
        """
        return list(
            pd.read_sql("SELECT * FROM main.sqlite_master WHERE type='table'", self.conn)["name"]
        )

    def set_current_process_time_zone(self):
        """
        This function queries a GTFS connection, finds the timezone of this
        database, and sets it in the TZ environment variable.  This is a
        process-global configuration, by the nature of the C library!

        Returns
        -------
        None

        Alters os.environ['TZ']
        """
        TZ = self.conn.execute("SELECT timezone FROM agencies LIMIT 1").fetchall()[0][0]
        # TODO!: This is dangerous (?).
        # In my opinion, we should get rid of this at some point (RK):
        return set_process_timezone(TZ)

    def get_timezone_pytz(self):
        return self._timezone

    def get_timezone_name(self):
        """
        Get name of the GTFS timezone

        Returns
        -------
        timezone_name : str
            name of the time zone, e.g. "Europe/Helsinki"
        """
        tz_name = self.conn.execute("SELECT timezone FROM agencies LIMIT 1").fetchone()
        if tz_name is None:
            raise ValueError("This database does not have a timezone defined.")
        return tz_name[0]

    def get_timezone_string(self, dt=None):
        """
        Return the timezone of the GTFS database object as a string.
        The assumed time when the timezone (difference) is computed
        is the download date of the file.
        This might not be optimal in all cases.

        So this function should return values like:
            "+0200" or "-1100"

        Parameters
        ----------
        dt : datetime.datetime, optional
            The (unlocalized) date when the timezone should be computed.
            Defaults first to download_date, and then to the runtime date.

        Returns
        -------
        timezone_string : str
        """
        if dt is None:
            download_date = self.meta.get("download_date")
            if download_date:
                dt = datetime.datetime.strptime(download_date, "%Y-%m-%d")
            else:
                dt = datetime.datetime.today()
        loc_dt = self._timezone.localize(dt)
        # get the timezone
        timezone_string = loc_dt.strftime("%z")
        return timezone_string

    def unixtime_seconds_to_gtfs_datetime(self, unixtime):
        """
        Convert unixtime to localized datetime

        Parameters
        ----------
        unixtime : int

        Returns
        -------
        gtfs_datetime: datetime.datetime
            time localized to gtfs_datetime's timezone
        """
        return datetime.datetime.fromtimestamp(unixtime, self._timezone)

    def unlocalized_datetime_to_ut_seconds(self, unlocalized_datetime):
        """
        Convert datetime (in GTFS timezone) to unixtime

        Parameters
        ----------
        unlocalized_datetime : datetime.datetime
            (tz coerced to GTFS timezone, should NOT be UTC.)

        Returns
        -------
        output : int (unixtime)
        """
        loc_dt = self._timezone.localize(unlocalized_datetime)
        unixtime_seconds = calendar.timegm(loc_dt.utctimetuple())
        return unixtime_seconds

    def get_day_start_ut(self, date):
        """
        Get day start time (as specified by GTFS) as unix time in seconds

        Parameters
        ----------
        date : str | unicode | datetime.datetime
            something describing the date

        Returns
        -------
        day_start_ut : int
            start time of the day in unixtime
        """
        if isinstance(date, string_types):
            date = datetime.datetime.strptime(date, "%Y-%m-%d")

        date_noon = datetime.datetime(date.year, date.month, date.day, 12, 0, 0)
        ut_noon = self.unlocalized_datetime_to_ut_seconds(date_noon)
        return ut_noon - 12 * 60 * 60  # this comes from GTFS: noon-12 hrs

    def get_trip_trajectories_within_timespan(self, start, end, use_shapes=True, filter_name=None):
        """
        Get complete trip data for visualizing public transport operation based on gtfs.

        Parameters
        ----------
        start: number
            Earliest position data to return (in unix time)
        end: number
            Latest position data to return (in unix time)
        use_shapes: bool, optional
            Whether or not shapes should be included
        filter_name: str
            Pick only routes having this name.

        Returns
        -------
        trips: dict
            trips['trips'] is a list whose each element (e.g. el = trips['trips'][0])
            is a dict with the following properties:
                el['lats'] -- list of latitudes
                el['lons'] -- list of longitudes
                el['times'] -- list of passage_times
                el['route_type'] -- type of vehicle as specified by GTFS
                el['name'] -- name of the route
        """
        trips = []
        trip_df = self.get_tripIs_active_in_range(start, end)
        print("gtfs_viz.py: fetched " + str(len(trip_df)) + " trip ids")
        shape_cache = {}

        # loop over all trips:
        for row in trip_df.itertuples():
            trip_I = row.trip_I
            day_start_ut = row.day_start_ut
            shape_id = row.shape_id

            trip = {}

            name, route_type = self.get_route_name_and_type_of_tripI(trip_I)
            trip["route_type"] = int(route_type)
            trip["name"] = str(name)

            if filter_name and (name != filter_name):
                continue

            stop_lats = []
            stop_lons = []
            stop_dep_times = []
            shape_breaks = []
            stop_seqs = []

            # get stop_data and store it:
            stop_time_df = self.get_trip_stop_time_data(trip_I, day_start_ut)
            for stop_row in stop_time_df.itertuples():
                stop_lats.append(float(stop_row.lat))
                stop_lons.append(float(stop_row.lon))
                stop_dep_times.append(float(stop_row.dep_time_ut))
                try:
                    stop_seqs.append(int(stop_row.seq))
                except TypeError:
                    stop_seqs.append(None)
                if use_shapes:
                    try:
                        shape_breaks.append(int(stop_row.shape_break))
                    except (TypeError, ValueError):
                        shape_breaks.append(None)

            if use_shapes:
                # get shape data (from cache, if possible)
                if shape_id not in shape_cache:
                    shape_cache[shape_id] = shapes.get_shape_points2(self.conn.cursor(), shape_id)
                shape_data = shape_cache[shape_id]
                # noinspection PyBroadException
                try:
                    trip["times"] = shapes.interpolate_shape_times(
                        shape_data["d"], shape_breaks, stop_dep_times
                    )
                    trip["lats"] = shape_data["lats"]
                    trip["lons"] = shape_data["lons"]
                    start_break = shape_breaks[0]
                    end_break = shape_breaks[-1]
                    trip["times"] = trip["times"][start_break : end_break + 1]
                    trip["lats"] = trip["lats"][start_break : end_break + 1]
                    trip["lons"] = trip["lons"][start_break : end_break + 1]
                except:
                    # In case interpolation fails:
                    trip["times"] = stop_dep_times
                    trip["lats"] = stop_lats
                    trip["lons"] = stop_lons
            else:
                trip["times"] = stop_dep_times
                trip["lats"] = stop_lats
                trip["lons"] = stop_lons
            trips.append(trip)
        return {"trips": trips}

    def get_stop_count_data(self, start_ut, end_ut):
        """
        Get stop count data.

        Parameters
        ----------
        start_ut : int
            start time in unixtime
        end_ut : int
            end time in unixtime

        Returns
        -------
        stopData : pandas.DataFrame
            each row in the stopData dataFrame is a dictionary with the following elements
                stop_I, count, lat, lon, name
            with data types
                (int, int, float, float, str)
        """
        # TODO! this function could perhaps be made a single sql query now with the new tables?
        trips_df = self.get_tripIs_active_in_range(start_ut, end_ut)
        # stop_I -> count, lat, lon, name
        stop_counts = Counter()

        # loop over all trips:
        for row in trips_df.itertuples():
            # get stop_data and store it:
            stops_seq = self.get_trip_stop_time_data(row.trip_I, row.day_start_ut)
            for stop_time_row in stops_seq.itertuples(index=False):
                if (stop_time_row.dep_time_ut >= start_ut) and (
                    stop_time_row.dep_time_ut <= end_ut
                ):
                    stop_counts[stop_time_row.stop_I] += 1

        all_stop_data = self.stops()
        counts = [stop_counts[stop_I] for stop_I in all_stop_data["stop_I"].values]

        all_stop_data.loc[:, "count"] = pd.Series(counts, index=all_stop_data.index)
        return all_stop_data

    def get_segment_count_data(self, start, end, use_shapes=True):
        """
        Get segment data including PTN vehicle counts per segment that are
        fully _contained_ within the interval (start, end)

        Parameters
        ----------
        start : int
            start time of the simulation in unix time
        end : int
            end time of the simulation in unix time
        use_shapes : bool, optional
            whether to include shapes (if available)

        Returns
        -------
        seg_data : list
            each element in the list is a dict containing keys:
                "trip_I", "lats", "lons", "shape_id", "stop_seqs", "shape_breaks"
        """
        cur = self.conn.cursor()
        # get all possible trip_ids that take place between start and end
        trips_df = self.get_tripIs_active_in_range(start, end)
        # stop_I -> count, lat, lon, name
        segment_counts = Counter()
        seg_to_info = {}
        # tripI_to_seq = "inverted segToShapeData"
        tripI_to_seq = defaultdict(list)

        # loop over all trips:
        for row in trips_df.itertuples():
            # get stop_data and store it:
            stops_df = self.get_trip_stop_time_data(row.trip_I, row.day_start_ut)
            for i in range(len(stops_df) - 1):
                (stop_I, dep_time_ut, s_lat, s_lon, s_seq, shape_break) = stops_df.iloc[i]
                (stop_I_n, dep_time_ut_n, s_lat_n, s_lon_n, s_seq_n, shape_break_n) = stops_df.iloc[
                    i + 1
                ]
                # test if _contained_ in the interval
                # overlap would read:
                #   (dep_time_ut <= end) and (start <= dep_time_ut_n)
                if (dep_time_ut >= start) and (dep_time_ut_n <= end):
                    seg = (stop_I, stop_I_n)
                    segment_counts[seg] += 1
                    if seg not in seg_to_info:
                        seg_to_info[seg] = {
                            "trip_I": row.trip_I,
                            "lats": [s_lat, s_lat_n],
                            "lons": [s_lon, s_lon_n],
                            "shape_id": row.shape_id,
                            "stop_seqs": [s_seq, s_seq_n],
                            "shape_breaks": [shape_break, shape_break_n],
                        }
                        tripI_to_seq[row.trip_I].append(seg)

        stop_names = {}
        for (stop_I, stop_J) in segment_counts.keys():
            for s in [stop_I, stop_J]:
                if s not in stop_names:
                    stop_names[s] = self.stop(s)["name"].values[0]

        seg_data = []
        for seg, count in segment_counts.items():
            segInfo = seg_to_info[seg]
            shape_breaks = segInfo["shape_breaks"]
            seg_el = {}
            if use_shapes and shape_breaks and shape_breaks[0] and shape_breaks[1]:
                shape = shapes.get_shape_between_stops(
                    cur, segInfo["trip_I"], shape_breaks=shape_breaks
                )
                seg_el["lats"] = segInfo["lats"][:1] + shape["lat"] + segInfo["lats"][1:]
                seg_el["lons"] = segInfo["lons"][:1] + shape["lon"] + segInfo["lons"][1:]
            else:
                seg_el["lats"] = segInfo["lats"]
                seg_el["lons"] = segInfo["lons"]
            seg_el["name"] = stop_names[seg[0]] + "-" + stop_names[seg[1]]
            seg_el["count"] = count
            seg_data.append(seg_el)
        return seg_data

    def get_all_route_shapes(self, use_shapes=True):
        """
        Get the shapes of all routes.

        Parameters
        ----------
        use_shapes : bool, optional
            by default True (i.e. use shapes as the name of the function indicates)
            if False (fall back to lats and longitudes)

        Returns
        -------
        routeShapes: list of dicts that should have the following keys
            name, type, agency, lats, lons
            with types
            list, list, str, list, list
        """
        cur = self.conn.cursor()

        # all shape_id:s corresponding to a route_I:
        # query = "SELECT DISTINCT name, shape_id, trips.route_I, route_type
        #          FROM trips LEFT JOIN routes USING(route_I)"
        # data1 = pd.read_sql_query(query, self.conn)
        # one (arbitrary) shape_id per route_I ("one direction") -> less than half of the routes
        query = (
            "SELECT routes.name as name, shape_id, route_I, trip_I, routes.type, "
            "        agency_id, agencies.name as agency_name, max(end_time_ds-start_time_ds) as trip_duration "
            "FROM trips "
            "LEFT JOIN routes "
            "USING(route_I) "
            "LEFT JOIN agencies "
            "USING(agency_I) "
            "GROUP BY routes.route_I"
        )
        data = pd.read_sql_query(query, self.conn)

        routeShapes = []
        for i, row in enumerate(data.itertuples()):
            datum = {
                "name": str(row.name),
                "type": int(row.type),
                "route_I": row.route_I,
                "agency": str(row.agency_id),
                "agency_name": str(row.agency_name),
            }
            # this function should be made also non-shape friendly (at this point)
            if use_shapes and row.shape_id:
                shape = shapes.get_shape_points2(cur, row.shape_id)
                lats = shape["lats"]
                lons = shape["lons"]
            else:
                stop_shape = self.get_trip_stop_coordinates(row.trip_I)
                lats = list(stop_shape["lat"])
                lons = list(stop_shape["lon"])
            datum["lats"] = [float(lat) for lat in lats]
            datum["lons"] = [float(lon) for lon in lons]
            routeShapes.append(datum)
        return routeShapes

    def get_tripIs_active_in_range(self, start, end):
        """
        Obtain from the (standard) GTFS database, list of trip_IDs (and other trip_related info)
        that are active between given 'start' and 'end' times.

        The start time of a trip is determined by the departure time at the last stop of the trip.
        The end time of a trip is determined by the arrival time at the last stop of the trip.

        Parameters
        ----------
        start, end : int
            the start and end of the time interval in unix time seconds

        Returns
        -------
        active_trips : pandas.DataFrame with columns
            trip_I, day_start_ut, start_time_ut, end_time_ut, shape_id
        """
        to_select = "trip_I, day_start_ut, start_time_ut, end_time_ut, shape_id "
        query = (
            "SELECT " + to_select + "FROM day_trips "
            "WHERE "
            "(end_time_ut > {start_ut} AND start_time_ut < {end_ut})".format(
                start_ut=start, end_ut=end
            )
        )
        return pd.read_sql_query(query, self.conn)

    def get_trip_counts_per_day(self):
        """
        Get trip counts per day between the start and end day of the feed.

        Returns
        -------
        trip_counts : pandas.DataFrame
            Has columns "date_str" (dtype str) "trip_counts" (dtype int)
        """
        query = "SELECT date, count(*) AS number_of_trips FROM day_trips GROUP BY date"
        # this yields the actual data
        trip_counts_per_day = pd.read_sql_query(query, self.conn, index_col="date")
        # the rest is simply code for filling out "gaps" in the time span
        # (necessary for some visualizations)
        max_day = trip_counts_per_day.index.max()
        min_day = trip_counts_per_day.index.min()
        min_date = datetime.datetime.strptime(min_day, "%Y-%m-%d")
        max_date = datetime.datetime.strptime(max_day, "%Y-%m-%d")
        num_days = (max_date - min_date).days
        dates = [min_date + datetime.timedelta(days=x) for x in range(num_days + 1)]
        trip_counts = []
        date_strings = []
        for date in dates:
            date_string = date.strftime("%Y-%m-%d")
            date_strings.append(date_string)
            try:
                value = trip_counts_per_day.loc[date_string, "number_of_trips"]
            except KeyError:
                # set value to 0 if dsut is not present, i.e. when no trips
                # take place on that day
                value = 0
            trip_counts.append(value)
        # check that all date_strings are included (move this to tests?)
        for date_string in trip_counts_per_day.index:
            assert date_string in date_strings
        data = {"date": dates, "date_str": date_strings, "trip_counts": trip_counts}
        return pd.DataFrame(data)

    def get_suitable_date_for_daily_extract(self, date=None, ut=False):
        """
        Parameters
        ----------
        date : str
        ut : bool
            Whether to return the date as a string or as a an int (seconds after epoch).

        Returns
        -------
        Selects suitable date for daily extract
        Iterates trough the available dates forward and backward from the download date accepting the first day that has
        at least 90 percent of the number of trips of the maximum date. The condition can be changed to something else.
        If the download date is out of range, the process will look through the dates from first to last.
        """
        daily_trips = self.get_trip_counts_per_day()
        max_daily_trips = daily_trips["trip_counts"].max(axis=0)
        if date in daily_trips["date_str"]:
            start_index = daily_trips[daily_trips["date_str"] == date].index.tolist()[0]
            daily_trips["old_index"] = daily_trips.index
            daily_trips["date_dist"] = abs(start_index - daily_trips.index)
            daily_trips = daily_trips.sort_values(by=["date_dist", "old_index"]).reindex()
        for row in daily_trips.itertuples():
            if row.trip_counts >= 0.9 * max_daily_trips:
                if ut:
                    return self.get_day_start_ut(row.date_str)
                else:
                    return row.date_str

    def get_weekly_extract_start_date(
        self, ut=False, weekdays_at_least_of_max=0.9, verbose=False, download_date_override=None
    ):
        """
        Find a suitable weekly extract start date (monday).
        The goal is to obtain as 'usual' week as possible.
        The weekdays of the weekly extract week should contain
        at least 0.9 of the total maximum of trips.

        Parameters
        ----------
        ut: return unixtime?
        weekdays_at_least_of_max: float

        download_date_override: str, semi-optional
            Download-date in format %Y-%m-%d, weeks close to this.
            Overrides the (possibly) recorded downloaded date in the database

        Returns
        -------
        date: int or str

        Raises
        ------
        error: RuntimeError
            If no download date could be found.
        """
        daily_trip_counts = self.get_trip_counts_per_day()
        if isinstance(download_date_override, str):
            search_start_date = datetime.datetime.strptime(download_date_override, "%Y-%m-%d")
        elif isinstance(download_date_override, datetime.datetime):
            search_start_date = download_date_override
        else:
            assert download_date_override is None
            download_date_str = self.meta["download_date"]
            if download_date_str == "":
                warnings.warn(
                    "Download date is not speficied in the database. "
                    "Download date used in GTFS."
                    + self.get_weekly_extract_start_date.__name__
                    + "() defaults to the smallest date when any operations take place."
                )
                search_start_date = daily_trip_counts["date"].min()
            else:
                search_start_date = datetime.datetime.strptime(download_date_str, "%Y-%m-%d")

        feed_min_date = daily_trip_counts["date"].min()
        feed_max_date = daily_trip_counts["date"].max()
        assert feed_max_date - feed_min_date >= datetime.timedelta(
            days=7
        ), "Dataset is not long enough for providing week long extracts"

        # get first a valid monday where the search for the week can be started:
        next_monday_from_search_start_date = search_start_date + timedelta(
            days=(7 - search_start_date.weekday())
        )
        if not (feed_min_date <= next_monday_from_search_start_date <= feed_max_date):
            warnings.warn(
                "The next monday after the (possibly user) specified download date is not present in the database."
                "Resorting to first monday after the beginning of operations instead."
            )
            next_monday_from_search_start_date = feed_min_date + timedelta(
                days=(7 - feed_min_date.weekday())
            )

        max_trip_count = daily_trip_counts["trip_counts"].quantile(0.95)
        # Take 95th percentile to omit special days, if any exist.

        threshold = weekdays_at_least_of_max * max_trip_count
        threshold_fulfilling_days = daily_trip_counts["trip_counts"] > threshold

        # look forward first
        # get the index of the trip:
        search_start_monday_index = daily_trip_counts[
            daily_trip_counts["date"] == next_monday_from_search_start_date
        ].index[0]

        # get starting point
        while_loop_monday_index = search_start_monday_index
        while len(daily_trip_counts.index) >= while_loop_monday_index + 7:
            if all(
                threshold_fulfilling_days[while_loop_monday_index : while_loop_monday_index + 5]
            ):
                row = daily_trip_counts.iloc[while_loop_monday_index]
                if ut:
                    return self.get_day_start_ut(row.date_str)
                else:
                    return row["date"]
            while_loop_monday_index += 7

        while_loop_monday_index = search_start_monday_index - 7
        # then backwards
        while while_loop_monday_index >= 0:
            if all(
                threshold_fulfilling_days[while_loop_monday_index : while_loop_monday_index + 5]
            ):
                row = daily_trip_counts.iloc[while_loop_monday_index]
                if ut:
                    return self.get_day_start_ut(row.date_str)
                else:
                    return row["date"]
            while_loop_monday_index -= 7

        raise RuntimeError("No suitable weekly extract start date could be determined!")

    def get_spreading_trips(
        self,
        start_time_ut,
        lat,
        lon,
        max_duration_ut=4 * 3600,
        min_transfer_time=30,
        use_shapes=False,
    ):
        """
        Starting from a specific point and time, get complete single source
        shortest path spreading dynamics as trips, or "events".

        Parameters
        ----------
        start_time_ut: number
            Start time of the spreading.
        lat: float
            latitude of the spreading seed location
        lon: float
            longitude of the spreading seed location
        max_duration_ut: int
            maximum duration of the spreading process (in seconds)
        min_transfer_time : int
            minimum transfer time in seconds
        use_shapes : bool
            whether to include shapes

        Returns
        -------
        trips: dict
            trips['trips'] is a list whose each element (e.g. el = trips['trips'][0])
            is a dict with the following properties:
                el['lats'] : list of latitudes
                el['lons'] : list of longitudes
                el['times'] : list of passage_times
                el['route_type'] : type of vehicle as specified by GTFS, or -1 if walking
                el['name'] : name of the route
        """
        from gtfspy.spreading.spreader import Spreader

        spreader = Spreader(
            self, start_time_ut, lat, lon, max_duration_ut, min_transfer_time, use_shapes
        )
        return spreader.spread()

    def get_closest_stop(self, lat, lon):
        """
        Get closest stop to a given location.

        Parameters
        ----------
        lat: float
            latitude coordinate of the location
        lon: float
            longitude coordinate of the location

        Returns
        -------
        stop_I: int
            the index of the stop in the database
        """
        cur = self.conn.cursor()
        min_dist = float("inf")
        min_stop_I = None
        rows = cur.execute("SELECT stop_I, lat, lon FROM stops")
        for stop_I, lat_s, lon_s in rows:
            dist_now = wgs84_distance(lat, lon, lat_s, lon_s)
            if dist_now < min_dist:
                min_dist = dist_now
                min_stop_I = stop_I
        return min_stop_I

    def get_stop_coordinates(self, stop_I):
        cur = self.conn.cursor()
        results = cur.execute(
            "SELECT lat, lon FROM stops WHERE stop_I={stop_I}".format(stop_I=stop_I)
        )
        lat, lon = results.fetchone()
        return lat, lon

    def get_bounding_box_by_stops(self, stop_Is, buffer_ratio=None):
        lats = []
        lons = []
        for stop_I in stop_Is:
            lat, lon = self.get_stop_coordinates(stop_I)
            lats.append(lat)
            lons.append(lon)
        min_lat = min(lats)
        max_lat = max(lats)
        min_lon = min(lons)
        max_lon = max(lons)
        lon_diff = 0
        lat_diff = 0

        if buffer_ratio:
            distance = buffer_ratio * wgs84_distance(min_lat, min_lon, max_lat, max_lon)
            lat_diff = wgs84_height(distance)
            lon_diff = wgs84_width(distance, (max_lat - min_lat) / 2 + min_lat)

        return {
            "lat_min": min_lat - lat_diff,
            "lat_max": max_lat + lat_diff,
            "lon_min": min_lon - lon_diff,
            "lon_max": max_lon + lon_diff,
        }

    def get_route_name_and_type_of_tripI(self, trip_I):
        """
        Get route short name and type

        Parameters
        ----------
        trip_I: int
            short trip index created when creating the database

        Returns
        -------
        name: str
            short name of the route, eg. 195N
        type: int
            route_type according to the GTFS standard
        """
        cur = self.conn.cursor()
        results = cur.execute(
            "SELECT name, type FROM routes JOIN trips USING(route_I) WHERE trip_I={trip_I}".format(
                trip_I=trip_I
            )
        )
        name, rtype = results.fetchone()
        return "%s" % str(name), int(rtype)

    def get_route_name_and_type(self, route_I):
        """
        Get route short name and type

        Parameters
        ----------
        route_I: int
            route index (database specific)

        Returns
        -------
        name: str
            short name of the route, eg. 195N
        type: int
            route_type according to the GTFS standard
        """
        cur = self.conn.cursor()
        results = cur.execute("SELECT name, type FROM routes WHERE route_I=(?)", (route_I,))
        name, rtype = results.fetchone()
        return name, int(rtype)

    def get_trip_stop_coordinates(self, trip_I):
        """
        Get coordinates for a given trip_I

        Parameters
        ----------
        trip_I : int
            the integer id of the trip

        Returns
        -------
        stop_coords : pandas.DataFrame
            with columns "lats" and "lons"
        """
        query = """SELECT lat, lon
                    FROM stop_times
                    JOIN stops
                    USING(stop_I)
                        WHERE trip_I={trip_I}
                    ORDER BY stop_times.seq""".format(
            trip_I=trip_I
        )
        stop_coords = pd.read_sql(query, self.conn)
        return stop_coords

    def get_trip_stop_time_data(self, trip_I, day_start_ut):
        """
        Obtain from the (standard) GTFS database, trip stop data
        (departure time in ut, lat, lon, seq, shape_break) as a pandas DataFrame

        Some filtering could be applied here, if only e.g. departure times
        corresponding within some time interval should be considered.

        Parameters
        ----------
        trip_I : int
            integer index of the trip
        day_start_ut : int
            the start time of the day in unix time (seconds)

        Returns
        -------
        df: pandas.DataFrame
            df has the following columns
            'departure_time_ut, lat, lon, seq, shape_break'
        """
        to_select = (
            "stop_I, "
            + str(day_start_ut)
            + "+dep_time_ds AS dep_time_ut, lat, lon, seq, shape_break"
        )
        str_to_run = (
            "SELECT "
            + to_select
            + """
                        FROM stop_times JOIN stops USING(stop_I)
                        WHERE (trip_I ={trip_I}) ORDER BY seq
                      """
        )
        str_to_run = str_to_run.format(trip_I=trip_I)
        return pd.read_sql_query(str_to_run, self.conn)

    def get_events_by_tripI_and_dsut(self, trip_I, day_start_ut, start_ut=None, end_ut=None):
        """
        Get trip data as a list of events (i.e. dicts).

        Parameters
        ----------
        trip_I : int
            shorthand index of the trip.
        day_start_ut : int
            the start time of the day in unix time (seconds)
        start_ut : int, optional
            consider only events that start after this time
            If not specified, this filtering is not applied.
        end_ut : int, optional
            Consider only events that end before this time
            If not specified, this filtering is not applied.

        Returns
        -------
        events: list of dicts
            each element contains the following data:
                from_stop: int (stop_I)
                to_stop: int (stop_I)
                dep_time_ut: int (in unix time)
                arr_time_ut: int (in unix time)
        """
        # for checking input:
        assert day_start_ut <= start_ut
        assert day_start_ut <= end_ut
        assert start_ut <= end_ut
        events = []
        # check that trip takes place on that day:
        if not self.tripI_takes_place_on_dsut(trip_I, day_start_ut):
            return events

        query = """SELECT stop_I, arr_time_ds+?, dep_time_ds+?
                    FROM stop_times JOIN stops USING(stop_I)
                    WHERE
                        (trip_I = ?)
                """
        params = [day_start_ut, day_start_ut, trip_I]
        if start_ut:
            query += "AND (dep_time_ds > ?-?)"
            params += [start_ut, day_start_ut]
        if end_ut:
            query += "AND (arr_time_ds < ?-?)"
            params += [end_ut, day_start_ut]
        query += "ORDER BY arr_time_ds"
        cur = self.conn.cursor()
        rows = cur.execute(query, params)
        stop_data = list(rows)
        for i in range(len(stop_data) - 1):
            event = {
                "from_stop": stop_data[i][0],
                "to_stop": stop_data[i + 1][0],
                "dep_time_ut": stop_data[i][2],
                "arr_time_ut": stop_data[i + 1][1],
            }
            events.append(event)
        return events

    def tripI_takes_place_on_dsut(self, trip_I, day_start_ut):
        """
        Check that a trip takes place during a day

        Parameters
        ----------
        trip_I : int
            index of the trip in the gtfs data base
        day_start_ut : int
            the starting time of the day in unix time (seconds)

        Returns
        -------
        takes_place: bool
            boolean value describing whether the trip takes place during
            the given day or not
        """
        query = "SELECT * FROM days WHERE trip_I=? AND day_start_ut=?"
        params = (trip_I, day_start_ut)
        cur = self.conn.cursor()
        rows = list(cur.execute(query, params))
        if len(rows) == 0:
            return False
        else:
            assert len(rows) == 1, "On a day, a trip_I should be present at most once"
            return True

    # unused and (untested) code:
    #
    # def get_tripIs_from_stopI_within_time_range(self, stop_I, day_start_ut, start_ut, end_ut):
    #     """
    #     Obtain a list of trip_Is that go through some stop during a given time.
    #
    #     Parameters
    #     ----------
    #     stop_I : int
    #         index of the stop to be considered
    #     day_start_ut : int
    #         start of the day in unix time (seconds)
    #     start_ut: int
    #         the first possible departure time from the stop
    #         in unix time (seconds)
    #     end_ut: int
    #         the last possible departure time from the stop
    #         in unix time (seconds)
    #
    #     Returns
    #     -------
    #     trip_Is: list
    #         list of integers (trip_Is)
    #     """
    #     start_ds = start_ut - day_start_ut
    #     end_ds = end_ut - day_start_ut
    #     # is the _distinct_ really required?
    #     query = "SELECT distinct(trip_I) " \
    #             "FROM days " \
    #             "JOIN stop_times " \
    #             "USING(trip_I) " \
    #             "WHERE (days.day_start_ut == ?)" \
    #             "AND (stop_times.stop_I=?) " \
    #             "AND (stop_times.dep_time_ds >= ?) " \
    #             "AND (stop_times.dep_time_ds <= ?)"
    #     params = (day_start_ut, stop_I, start_ds, end_ds)
    #     cur = self.conn.cursor()
    #     trip_Is = [el[0] for el in cur.execute(query, params)]
    #     return trip_Is

    def day_start_ut(self, ut):
        """
        Convert unixtime to unixtime on GTFS start-of-day.

        GTFS defines the start of a day as "noon minus 12 hours" to solve
        most DST-related problems. This means that on DST-changing days,
        the day start isn't midnight. This function isn't idempotent.
        Running it twice on the "move clocks backwards" day will result in
        being one day too early.

        Parameters
        ----------
        ut: int
            Unixtime

        Returns
        -------
        ut: int
            Unixtime corresponding to start of day
        """
        # set timezone to the one of gtfs
        old_tz = self.set_current_process_time_zone()
        ut = time.mktime(time.localtime(ut)[:3] + (12, 00, 0, 0, 0, -1)) - 43200
        set_process_timezone(old_tz)
        return ut

    def increment_day_start_ut(self, day_start_ut, n_days=1):
        """Increment the GTFS-definition of "day start".

        Parameters
        ----------
        day_start_ut : int
            unixtime of the previous start of day.  If this time is between
            12:00 or greater, there *will* be bugs.  To solve this, run the
            input through day_start_ut first.
        n_days: int
            number of days to increment
        """
        old_tz = self.set_current_process_time_zone()
        day0 = time.localtime(day_start_ut + 43200)  # time of noon
        dayN = (
            time.mktime(day0[:2] + (day0[2] + n_days,) + (12, 00, 0, 0, 0, -1))  # YYYY, MM  # DD
            - 43200
        )  # HHMM, etc.  Minus 12 hours.
        set_process_timezone(old_tz)
        return dayN

    def _get_possible_day_starts(self, start_ut, end_ut, max_time_overnight=None):
        """
        Get all possible day start times between start_ut and end_ut
        Currently this function is used only by get_tripIs_within_range_by_dsut

        Parameters
        ----------
        start_ut : list<int>
            start time in unix time
        end_ut : list<int>
            end time in unix time
        max_time_overnight : list<int>
            the maximum length of time that a trip can take place on
            during the next day (i.e. after midnight run times like 25:35)

        Returns
        -------
        day_start_times_ut : list
            list of ints (unix times in seconds) for returning all possible day
            start times
        start_times_ds : list
            list of ints (unix times in seconds) stating the valid start time in
            day seconds
        end_times_ds : list
            list of ints (unix times in seconds) stating the valid end times in
            day_seconds
        """
        if max_time_overnight is None:
            # 7 hours:
            max_time_overnight = 7 * 60 * 60

        # sanity checks for the timezone parameter
        # assert timezone < 14
        # assert timezone > -14
        # tz_seconds = int(timezone*3600)
        assert start_ut < end_ut
        start_day_ut = self.day_start_ut(start_ut)
        # start_day_ds = int(start_ut+tz_seconds) % seconds_in_a_day  #??? needed?
        start_day_ds = start_ut - start_day_ut
        # assert (start_day_ut+tz_seconds) % seconds_in_a_day == 0
        end_day_ut = self.day_start_ut(end_ut)
        # end_day_ds = int(end_ut+tz_seconds) % seconds_in_a_day    #??? needed?
        # end_day_ds = end_ut - end_day_ut
        # assert (end_day_ut+tz_seconds) % seconds_in_a_day == 0

        # If we are early enough in a day that we might have trips from
        # the previous day still running, decrement the start day.
        if start_day_ds < max_time_overnight:
            start_day_ut = self.increment_day_start_ut(start_day_ut, n_days=-1)

        # day_start_times_ut = range(start_day_ut, end_day_ut+seconds_in_a_day, seconds_in_a_day)

        # Create a list of all possible day start times.  This is roughly
        # range(day_start_ut, day_end_ut+1day, 1day).
        day_start_times_ut = [start_day_ut]
        while day_start_times_ut[-1] < end_day_ut:
            day_start_times_ut.append(self.increment_day_start_ut(day_start_times_ut[-1]))

        start_times_ds = []
        end_times_ds = []
        # For every possible day start:
        for dsut in day_start_times_ut:
            # start day_seconds starts at either zero, or time - daystart
            day_start_ut = max(0, start_ut - dsut)
            start_times_ds.append(day_start_ut)
            # end day_seconds is time-day_start
            day_end_ut = end_ut - dsut
            end_times_ds.append(day_end_ut)
        # Return three tuples which can be zip:ped together.
        return day_start_times_ut, start_times_ds, end_times_ds

    def get_tripIs_within_range_by_dsut(self, start_time_ut, end_time_ut):
        """
        Obtain a list of trip_Is that take place during a time interval.
        The trip needs to be only partially overlapping with the given time interval.
        The grouping by dsut (day_start_ut) is required as same trip_I could
        take place on multiple days.

        Parameters
        ----------
        start_time_ut : int
            start of the time interval in unix time (seconds)
        end_time_ut: int
            end of the time interval in unix time (seconds)

        Returns
        -------
        trip_I_dict: dict
            keys: day_start_times to list of integers (trip_Is)
        """
        cur = self.conn.cursor()
        assert start_time_ut <= end_time_ut
        dst_ut, st_ds, et_ds = self._get_possible_day_starts(start_time_ut, end_time_ut, 7)
        # noinspection PyTypeChecker
        assert len(dst_ut) >= 0
        trip_I_dict = {}
        for day_start_ut, start_ds, end_ds in zip(dst_ut, st_ds, et_ds):
            query = """
                        SELECT distinct(trip_I)
                        FROM days
                            JOIN trips
                            USING(trip_I)
                        WHERE
                            (days.day_start_ut == ?)
                            AND (
                                    (trips.start_time_ds <= ?)
                                    AND
                                    (trips.end_time_ds >= ?)
                                )
                        """
            params = (day_start_ut, end_ds, start_ds)
            trip_Is = [el[0] for el in cur.execute(query, params)]
            if len(trip_Is) > 0:
                trip_I_dict[day_start_ut] = trip_Is
        return trip_I_dict

    def stops(self):
        """
        Get all stop data as a pandas DataFrame

        Returns
        -------
        df: pandas.DataFrame
        """
        return self.get_table("stops")

    def stop(self, stop_I):
        """
        Get all stop data as a pandas DataFrame for all stops, or an individual stop'

        Parameters
        ----------
        stop_I : int
            stop index

        Returns
        -------
        stop: pandas.DataFrame
        """
        return pd.read_sql_query(
            "SELECT * FROM stops WHERE stop_I={stop_I}".format(stop_I=stop_I), self.conn
        )

    def add_coordinates_to_df(self, df, join_column="stop_I", lat_name="lat", lon_name="lon"):
        assert join_column in df.columns
        stops_df = self.stops()
        coord_df = stops_df[["stop_I", "lat", "lon"]]

        df_merged = pd.merge(coord_df, df, left_on="stop_I", right_on=join_column)

        df_merged.drop(["stop_I"], axis=1, inplace=True)
        df_merged3 = df_merged.rename(columns={"lat": lat_name, "lon": lon_name})
        return df_merged3

    def get_n_stops(self):
        return pd.read_sql_query("SELECT count(*) from stops;", self.conn).values[0, 0]

    def get_modes(self):
        modes = list(
            pd.read_sql_query("SELECT distinct(type) from routes;", self.conn).values.flatten()
        )
        return modes

    def get_stops_for_route_type(self, route_type):
        """
        Parameters
        ----------
        route_type: int

        Returns
        -------
        stops: pandas.DataFrame

        """
        if route_type is WALK:
            return self.stops()
        else:
            return pd.read_sql_query(
                "SELECT DISTINCT stops.* "
                "FROM stops JOIN stop_times ON stops.stop_I == stop_times.stop_I "
                "           JOIN trips ON stop_times.trip_I = trips.trip_I"
                "           JOIN routes ON trips.route_I == routes.route_I "
                "WHERE routes.type=(?)",
                self.conn,
                params=(route_type,),
            )

    def get_stops_connected_to_stop(self):
        pass

    def generate_routable_transit_events(
        self, start_time_ut=None, end_time_ut=None, route_type=None
    ):
        """
        Generates events that take place during a time interval [start_time_ut, end_time_ut].
        Each event needs to be only partially overlap the given time interval.
        Does not include walking events.
        This is just a quick and dirty implementation to get a way of quickly get a
        method for generating events compatible with the routing algorithm

        Parameters
        ----------
        start_time_ut: int
        end_time_ut: int
        route_type: ?

        Yields
        ------
        event: namedtuple
            containing:
                dep_time_ut: int
                arr_time_ut: int
                from_stop_I: int
                to_stop_I: int
                trip_I : int
                route_type : int
                seq: int
        """
        from gtfspy.networks import temporal_network

        df = temporal_network(
            self, start_time_ut=start_time_ut, end_time_ut=end_time_ut, route_type=route_type
        )
        df.sort_values("dep_time_ut", ascending=False, inplace=True)

        for row in df.itertuples():
            yield row

    def get_transit_events(self, start_time_ut=None, end_time_ut=None, route_type=None):
        """
        Obtain a list of events that take place during a time interval.
        Each event needs to be only partially overlap the given time interval.
        Does not include walking events.

        Parameters
        ----------
        start_time_ut : int
            start of the time interval in unix time (seconds)
        end_time_ut: int
            end of the time interval in unix time (seconds)
        route_type: int
            consider only events for this route_type

        Returns
        -------
        events: pandas.DataFrame
            with the following columns and types
                dep_time_ut: int
                arr_time_ut: int
                from_stop_I: int
                to_stop_I: int
                trip_I : int
                shape_id : int
                route_type : int

        See also
        --------
        get_transit_events_in_time_span : an older version of the same thing
        """
        table_name = self._get_day_trips_table_name()
        event_query = (
            "SELECT stop_I, seq, trip_I, route_I, routes.route_id AS route_id, routes.type AS route_type, "
            "shape_id, day_start_ut+dep_time_ds AS dep_time_ut, day_start_ut+arr_time_ds AS arr_time_ut "
            "FROM " + table_name + " "
            "JOIN trips USING(trip_I) "
            "JOIN routes USING(route_I) "
            "JOIN stop_times USING(trip_I)"
        )

        where_clauses = []
        if end_time_ut:
            where_clauses.append(
                table_name + ".start_time_ut< {end_time_ut}".format(end_time_ut=end_time_ut)
            )
            where_clauses.append("dep_time_ut  <={end_time_ut}".format(end_time_ut=end_time_ut))
        if start_time_ut:
            where_clauses.append(
                table_name + ".end_time_ut  > {start_time_ut}".format(start_time_ut=start_time_ut)
            )
            where_clauses.append(
                "arr_time_ut  >={start_time_ut}".format(start_time_ut=start_time_ut)
            )
        if route_type is not None:
            assert route_type in ALL_ROUTE_TYPES
            where_clauses.append("routes.type={route_type}".format(route_type=route_type))
        if len(where_clauses) > 0:
            event_query += " WHERE "
            for i, where_clause in enumerate(where_clauses):
                if i is not 0:
                    event_query += " AND "
                event_query += where_clause
        # ordering is required for later stages
        event_query += " ORDER BY trip_I, day_start_ut+dep_time_ds;"
        events_result = pd.read_sql_query(event_query, self.conn)
        # 'filter' results so that only real "events" are taken into account
        from_indices = numpy.nonzero(
            (events_result["trip_I"][:-1].values == events_result["trip_I"][1:].values)
            * (events_result["seq"][:-1].values < events_result["seq"][1:].values)
        )[0]
        to_indices = from_indices + 1
        # these should have same trip_ids
        assert (
            events_result["trip_I"][from_indices].values
            == events_result["trip_I"][to_indices].values
        ).all()
        trip_Is = events_result["trip_I"][from_indices]
        from_stops = events_result["stop_I"][from_indices]
        to_stops = events_result["stop_I"][to_indices]
        shape_ids = events_result["shape_id"][from_indices]
        dep_times = events_result["dep_time_ut"][from_indices]
        arr_times = events_result["arr_time_ut"][to_indices]
        route_types = events_result["route_type"][from_indices]
        route_ids = events_result["route_id"][from_indices]
        route_Is = events_result["route_I"][from_indices]
        durations = arr_times.values - dep_times.values
        assert (durations >= 0).all()
        from_seqs = events_result["seq"][from_indices]
        to_seqs = events_result["seq"][to_indices]
        data_tuples = zip(
            from_stops,
            to_stops,
            dep_times,
            arr_times,
            shape_ids,
            route_types,
            route_ids,
            trip_Is,
            durations,
            from_seqs,
            to_seqs,
            route_Is,
        )
        columns = [
            "from_stop_I",
            "to_stop_I",
            "dep_time_ut",
            "arr_time_ut",
            "shape_id",
            "route_type",
            "route_id",
            "trip_I",
            "duration",
            "from_seq",
            "to_seq",
            "route_I",
        ]
        df = pd.DataFrame.from_records(data_tuples, columns=columns)
        return df

    def get_route_difference_with_other_db(
        self, other_gtfs, start_time, end_time, uniqueness_threshold=None, uniqueness_ratio=None
    ):
        """
        Compares the routes based on stops in the schedule with the routes in another db and returns the ones without match.
        Uniqueness thresholds or ratio can be used to allow small differences
        :param uniqueness_threshold:
        :param uniqueness_ratio:
        :return:
        """
        from gtfspy.stats import frequencies_by_generated_route

        this_df = frequencies_by_generated_route(self, start_time, end_time)
        other_df = frequencies_by_generated_route(other_gtfs, start_time, end_time)
        this_routes = {x: set(x.split(",")) for x in this_df["route"]}
        other_routes = {x: set(x.split(",")) for x in other_df["route"]}
        # this_df["route_set"] = this_df.apply(lambda x: set(x.route.split(',')), axis=1)
        # other_df["route_set"] = other_df.apply(lambda x: set(x.route.split(',')), axis=1)

        this_uniques = list(this_routes.keys())
        other_uniques = list(other_routes.keys())
        print("initial routes A:", len(this_uniques))
        print("initial routes B:", len(other_uniques))
        for i_key, i in this_routes.items():
            for j_key, j in other_routes.items():
                union = i | j
                intersection = i & j
                symmetric_difference = i ^ j
                if uniqueness_ratio:
                    if len(intersection) / len(union) >= uniqueness_ratio:
                        try:
                            this_uniques.remove(i_key)
                            this_df = this_df[this_df["route"] != i_key]
                        except ValueError:
                            pass
                        try:
                            other_uniques.remove(j_key)
                            other_df = other_df[other_df["route"] != j_key]
                        except ValueError:
                            pass

        print("unique routes A", len(this_df))
        print("unique routes B", len(other_df))
        return this_df, other_df

    def get_section_difference_with_other_db(self, other_conn, start_time, end_time):
        query = """SELECT from_stop_I, to_stop_I, sum(n_trips) AS n_trips, count(*) AS n_routes,
                    group_concat(route_id) AS all_routes FROM
                    (SELECT route_id, from_stop_I, to_stop_I, count(*) AS n_trips FROM
                    (SELECT stop_I AS from_stop_I, seq, trip_I FROM stop_times
                    WHERE dep_time_ds >= %s) t1,
                    (SELECT stop_I AS to_stop_I, seq, trip_I  FROM stop_times
                    WHERE arr_time_ds <= %s) t2,
                    trips,
                    routes
                    WHERE t1.seq +1 = t2.seq AND t1.trip_I = t2.trip_I
                    AND t1.trip_I = trips.trip_I AND trips.route_I = routes.route_I
                    GROUP BY from_stop_I, to_stop_I, routes.route_I
                    ORDER BY route_id) sq1
                    GROUP BY from_stop_I, to_stop_I""" % (
            start_time,
            end_time,
        )

        prev_df = None
        result = pd.DataFrame
        for conn in [self.conn, other_conn]:
            df = conn.execute_custom_query_pandas(query)
            df.set_index(["from_stop_I", "to_stop_I"], inplace=True, drop=True)
            if prev_df is not None:
                result = prev_df.merge(
                    df, how="outer", left_index=True, right_index=True, suffixes=["_old", "_new"]
                )
                break

            prev_df = df
        for suffix in ["_new", "_old"]:
            result["all_routes" + suffix] = result["all_routes" + suffix].fillna(value="")
            result["all_routes" + suffix] = result["all_routes" + suffix].apply(
                lambda x: x.split(",")
            )
        result.reset_index(inplace=True)
        result.fillna(value=0, inplace=True)
        for column in ["n_trips", "n_routes"]:
            result["diff_" + column] = result[column + "_new"] - result[column + "_old"]
        return result

    def get_straight_line_transfer_distances(self, stop_I=None):
        """
        Get (straight line) distances to stations that can be transferred to.

        Parameters
        ----------
        stop_I : int, optional
            If not specified return all possible transfer distances

        Returns
        -------
        distances: pandas.DataFrame
            each row has the following items
                from_stop_I: int
                to_stop_I: int
                d: float or int #distance in meters
        """
        if stop_I is not None:
            query = """ SELECT from_stop_I, to_stop_I, d
                        FROM stop_distances
                            WHERE
                                from_stop_I=?
                    """
            params = ("{stop_I}".format(stop_I=stop_I),)
        else:
            query = """ SELECT from_stop_I, to_stop_I, d
                        FROM stop_distances
                    """
            params = None
        stop_data_df = pd.read_sql_query(query, self.conn, params=params)
        return stop_data_df

    def update_stats(self, stats):
        self.meta.update(stats)
        self.meta["stats_calc_at_ut"] = time.time()

    def get_approximate_schedule_time_span_in_ut(self):
        """
        Return conservative estimates of start_time_ut and end_time_uts.
        All trips, events etc. should start after start_time_ut_conservative and end before end_time_ut_conservative

        Returns
        -------
        start_time_ut_conservative : int
        end_time_ut_conservative : int
        """
        first_day_start_ut, last_day_start_ut = self.get_day_start_ut_span()
        # 28 (instead of 24) comes from the GTFS standard
        return first_day_start_ut, last_day_start_ut + 28 * 3600

    def get_day_start_ut_span(self):
        """
        Return the first and last day_start_ut

        Returns
        -------
        first_day_start_ut: int
        last_day_start_ut: int
        """
        cur = self.conn.cursor()
        first_day_start_ut, last_day_start_ut = cur.execute(
            "SELECT min(day_start_ut), max(day_start_ut) FROM days;"
        ).fetchone()
        return first_day_start_ut, last_day_start_ut

    def get_min_date(self):
        cur = self.conn.cursor()
        return cur.execute("SELECT min(date) FROM days").fetchone()[0]

    def get_max_date(self):
        cur = self.conn.cursor()
        return cur.execute("SELECT max(date) FROM days").fetchone()[0]

    def print_validation_warnings(self):
        """
        See Validator.validate for more information.

        Returns
        -------
        warnings_container: validator.TimetableValidationWarningsContainer
        """
        from .timetable_validator import TimetableValidator

        validator = TimetableValidator(self)
        return validator.validate_and_get_warnings()

    def execute_custom_query(self, query):
        return self.conn.cursor().execute(query)

    def execute_custom_query_pandas(self, query):
        return pd.read_sql(query, self.conn)

    def get_stats(self):
        from gtfspy import stats

        return stats.get_stats(self)

    def _get_day_trips_table_name(self):
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='day_trips2'"
        )
        if len(cur.fetchall()) > 0:
            table_name = "day_trips2"
        else:
            table_name = "day_trips"
        return table_name

    # TODO: The following methods could be moved to a "edit gtfs" -module
    def homogenize_stops_table_with_other_db(self, source):
        """
        This function takes an external database, looks of common stops and adds the missing stops to both databases.
        In addition the stop_pair_I column is added. This id links the stops between these two sources.
        :param source: directory of external database
        :return:
        """
        cur = self.conn.cursor()
        self.attach_gtfs_database(source)

        query_inner_join = """SELECT t1.*
                              FROM stops t1
                              INNER JOIN other.stops t2
                              ON t1.stop_id=t2.stop_id
                              AND find_distance(t1.lon, t1.lat, t2.lon, t2.lat) <= 50"""
        df_inner_join = self.execute_custom_query_pandas(query_inner_join)
        print("number of common stops: ", len(df_inner_join.index))
        df_not_in_other = self.execute_custom_query_pandas(
            "SELECT * FROM stops EXCEPT " + query_inner_join
        )
        print("number of stops missing in second feed: ", len(df_not_in_other.index))
        df_not_in_self = self.execute_custom_query_pandas(
            "SELECT * FROM other.stops EXCEPT " + query_inner_join.replace("t1.*", "t2.*")
        )
        print("number of stops missing in first feed: ", len(df_not_in_self.index))
        try:
            self.execute_custom_query("""ALTER TABLE stops ADD COLUMN stop_pair_I INT """)

            self.execute_custom_query("""ALTER TABLE other.stops ADD COLUMN stop_pair_I INT """)
        except sqlite3.OperationalError:
            pass
        stop_id_stub = "added_stop_"
        counter = 0
        rows_to_update_self = []
        rows_to_update_other = []
        rows_to_add_to_self = []
        rows_to_add_to_other = []

        for items in df_inner_join.itertuples(index=False):
            rows_to_update_self.append((counter, items[1]))
            rows_to_update_other.append((counter, items[1]))
            counter += 1

        for items in df_not_in_other.itertuples(index=False):
            rows_to_update_self.append((counter, items[1]))
            rows_to_add_to_other.append(
                (stop_id_stub + str(counter),)
                + tuple(items[x] for x in [2, 3, 4, 5, 6, 8, 9])
                + (counter,)
            )
            counter += 1

        for items in df_not_in_self.itertuples(index=False):
            rows_to_update_other.append((counter, items[1]))
            rows_to_add_to_self.append(
                (stop_id_stub + str(counter),)
                + tuple(items[x] for x in [2, 3, 4, 5, 6, 8, 9])
                + (counter,)
            )
            counter += 1

        query_add_row = """INSERT INTO stops(
                                    stop_id,
                                    code,
                                    name,
                                    desc,
                                    lat,
                                    lon,
                                    location_type,
                                    wheelchair_boarding,
                                    stop_pair_I) VALUES (%s) """ % (
            ", ".join(["?" for x in range(9)])
        )

        query_update_row = """UPDATE stops SET stop_pair_I=? WHERE stop_id=?"""
        print("adding rows to databases")
        cur.executemany(query_add_row, rows_to_add_to_self)
        cur.executemany(query_update_row, rows_to_update_self)
        cur.executemany(query_add_row.replace("stops", "other.stops"), rows_to_add_to_other)
        cur.executemany(query_update_row.replace("stops", "other.stops"), rows_to_update_other)
        self.conn.commit()
        print("finished")

    def replace_stop_i_with_stop_pair_i(self):
        cur = self.conn.cursor()
        queries = [
            "UPDATE stop_times SET stop_I = "
            "(SELECT stops.stop_pair_I AS stop_I FROM stops WHERE stops.stop_I = stop_times.stop_I)",
            # Replace stop_distances
            "ALTER TABLE stop_distances RENAME TO stop_distances_old",
            "CREATE TABLE stop_distances (from_stop_I INT, to_stop_I INT, d INT, d_walk INT, min_transfer_time INT, "
            "timed_transfer INT, UNIQUE (from_stop_I, to_stop_I))",
            "INSERT INTO stop_distances(from_stop_I, to_stop_I, d, d_walk, min_transfer_time, timed_transfer) "
            "SELECT f_stop.stop_pair_I AS from_stop_I, t_stop.stop_pair_I AS to_stop_I, d, d_walk, min_transfer_time, "
            "timed_transfer "
            "FROM "
            "(SELECT from_stop_I, to_stop_I, d, d_walk, min_transfer_time, "
            "timed_transfer "
            "FROM stop_distances_old) sd_o "
            "LEFT JOIN "
            "(SELECT stop_I, stop_pair_I FROM stops) f_stop "
            "ON sd_o.from_stop_I = f_stop.stop_I "
            " JOIN "
            "(SELECT stop_I, stop_pair_I FROM stops) t_stop "
            "ON sd_o.to_stop_I = t_stop.stop_I ;",
            "DROP TABLE stop_distances_old",
            # Replace stops table with other
            "ALTER TABLE stops RENAME TO stops_old",
            "CREATE TABLE stops (stop_I INTEGER PRIMARY KEY, stop_id TEXT UNIQUE NOT NULL, code TEXT, name TEXT, "
            "desc TEXT, lat REAL, lon REAL, parent_I INT, location_type INT, wheelchair_boarding BOOL, "
            "self_or_parent_I INT, old_stop_I INT)",
            "INSERT INTO stops(stop_I, stop_id, code, name, desc, lat, lon, parent_I, location_type, "
            "wheelchair_boarding, self_or_parent_I, old_stop_I) "
            "SELECT stop_pair_I AS stop_I, stop_id, code, name, desc, lat, lon, parent_I, location_type, "
            "wheelchair_boarding, self_or_parent_I, stop_I AS old_stop_I "
            "FROM stops_old;",
            "DROP TABLE stops_old",
            "CREATE INDEX idx_stops_sid ON stops (stop_I)",
        ]
        for query in queries:
            cur.execute(query)
        self.conn.commit()

    def regenerate_parent_stop_I(self):
        raise NotImplementedError
        # get max stop_I
        cur = self.conn.cursor()

        query = "SELECT stop_I FROM stops ORDER BY stop_I DESC LIMIT 1"
        max_stop_I = cur.execute(query).fetchall()[0]

        query_update_row = """UPDATE stops SET parent_I=? WHERE parent_I=?"""

    def add_stops_from_csv(self, csv_dir):
        stops_to_add = pd.read_csv(csv_dir, encoding="utf-8")
        assert all(
            [x in stops_to_add.columns for x in ["stop_id", "code", "name", "desc", "lat", "lon"]]
        )
        for s in stops_to_add.itertuples():
            self.add_stop(s.stop_id, s.code, s.name, s.desc, s.lat, s.lon)

    def add_stop(self, stop_id, code, name, desc, lat, lon):
        cur = self.conn.cursor()
        query_add_row = (
            "INSERT INTO stops( stop_id, code, name, desc, lat, lon) " "VALUES (?, ?, ?, ?, ?, ?)"
        )
        cur.executemany(query_add_row, [[stop_id, code, name, desc, lat, lon]])
        self.conn.commit()

    def recalculate_stop_distances(self, max_distance):
        from gtfspy.calc_transfers import calc_transfers

        calc_transfers(self.conn, max_distance)

    def attach_gtfs_database(self, gtfs_dir):
        cur = self.conn.cursor()
        cur.execute("ATTACH '%s' AS 'other'" % str(gtfs_dir))
        cur.execute("PRAGMA database_list")
        print("GTFS database attached:", cur.fetchall())

    def update_stop_coordinates(self, stop_updates):
        """

        :param stop_updates: DataFrame
        :return:
        """
        cur = self.conn.cursor()

        stop_values = [
            (values.lat, values.lon, values.stop_id) for values in stop_updates.itertuples()
        ]
        cur.executemany("""UPDATE stops SET lat = ?, lon = ? WHERE stop_id = ?""", stop_values)
        self.conn.commit()


class GTFSMetadata(object):
    """
    This provides dictionary protocol for updating GTFS metadata ("meta table").

    TODO: does not rep ???
    """

    def __init__(self, conn):
        self._conn = conn

    def __getitem__(self, key):
        val = self._conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if not val:
            raise KeyError("This GTFS does not have metadata: %s" % key)
        return val[0]

    def __setitem__(self, key, value):
        """Get metadata from the DB"""
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        self._conn.execute(
            "INSERT OR REPLACE INTO metadata " "(key, value) VALUES (?, ?)", (key, value)
        ).fetchone()
        self._conn.commit()

    def __delitem__(self, key):
        self._conn.execute("DELETE FROM metadata WHERE key=?", (key,)).fetchone()
        self._conn.commit()

    def __iter__(self):
        cur = self._conn.execute("SELECT key FROM metadata ORDER BY key")
        return (x[0] for x in cur)

    def __contains__(self, key):
        val = self._conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return val is not None

    def get(self, key, default=None):
        val = self._conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if not val:
            return default
        return val[0]

    def items(self):
        cur = self._conn.execute("SELECT key, value FROM metadata ORDER BY key")
        return cur

    def keys(self):
        cur = self._conn.execute("SELECT key FROM metadata ORDER BY key")
        return cur

    def values(self):
        cur = self._conn.execute("SELECT value FROM metadata ORDER BY key")
        return cur

    def update(self, dict_):
        # Would be more efficient to do it in a new query here, but
        # preferring simplicity.  metadata updates are probably
        # infrequent.
        if hasattr(dict_, "items"):
            for key, value in dict_.items():
                self[key] = value
        else:
            for key, value in dict_:
                self[key] = value


def main(cmd, args):
    from gtfspy import filter

    # noinspection PyPackageRequirements
    if cmd == "stats":
        print(args[0])
        G = GTFS(args[0])
        stats = G.get_stats()
        G.update_stats(stats)
        for row in G.meta.items():
            print(row)
    elif cmd == "validate":
        G = GTFS(args[0])
        G.print_validation_warnings()
    elif cmd == "metadata-list":
        # print args[0]  # need to not print to be valid json on stdout
        G = GTFS(args[0])
        # for row in G.meta.items():
        #    print row
        stats = dict(G.meta.items())
        import json

        print(json.dumps(stats, sort_keys=True, indent=4, separators=(",", ": ")))
    elif cmd == "make-daily":
        from_db = args[0]
        g = GTFS(from_db)
        to_db = args[1]
        download_date = g.meta["download_date"]
        d = datetime.datetime.strptime(download_date, "%Y-%m-%d").date()
        start_time = d + datetime.timedelta(7 - d.isoweekday() + 1)  # inclusive
        end_time = d + datetime.timedelta(7 - d.isoweekday() + 1 + 1)  # exclusive
        filter.filter_extract(g, to_db, start_date=start_time, end_date=end_time)
    elif cmd == "make-weekly":
        from_db = args[0]
        g = GTFS(from_db)
        to_db = args[1]
        download_date = g.meta["download_date"]
        d = datetime.datetime.strptime(download_date, "%Y-%m-%d").date()
        start_time = d + datetime.timedelta(7 - d.isoweekday() + 1)  # inclusive
        end_time = d + datetime.timedelta(7 - d.isoweekday() + 1 + 7)  # exclusive
        print(start_time, end_time)
        filter.filter_extract(g, to_db, start_date=start_time, end_date=end_time)
    elif cmd == "spatial-extract":
        try:
            from_db = args[0]
            lat = float(args[1])
            lon = float(args[2])
            radius_in_km = float(args[3])
            to_db = args[4]
        except Exception as e:
            print(
                "spatial-extract usage: python gtfs.py spatial-extract fromdb.sqlite center_lat center_lon "
                "radius_in_km todb.sqlite"
            )
            raise e
        logging.basicConfig(level=logging.INFO)
        logging.info("Loading initial database")
        g = GTFS(from_db)
        filter.filter_extract(
            g, to_db, buffer_distance=radius_in_km * 1000, buffer_lat=lat, buffer_lon=lon
        )
    elif cmd == "interact":
        # noinspection PyUnusedLocal
        G = GTFS(args[0])
        # noinspection PyPackageRequirements
        import IPython

        IPython.embed()
    elif "export_shapefile" in cmd:
        from gtfspy.util import write_shapefile

        from_db = args[
            0
        ]  # '/m/cs/project/networks/jweckstr/transit/scratch/proc_latest/helsinki/2016-04-06/main.day.sqlite'
        shapefile_path = args[1]  # '/m/cs/project/networks/jweckstr/TESTDATA/helsinki_routes.shp'
        g = GTFS(from_db)
        if cmd == "export_shapefile_routes":
            data = g.get_all_route_shapes(use_shapes=True)

        elif cmd == "export_shapefile_segment_counts":
            date = args[2]  # '2016-04-06'
            d = datetime.datetime.strptime(date, "%Y-%m-%d").date()
            day_start = g.get_day_start_ut(d + datetime.timedelta(7 - d.isoweekday() + 1))
            start_time = day_start + 3600 * 7
            end_time = day_start + 3600 * 8
            data = g.get_segment_count_data(start_time, end_time, use_shapes=True)

        write_shapefile(data, shapefile_path)

    else:
        print("Unrecognized command: %s" % cmd)
        exit(1)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
