from __future__ import print_function
from __future__ import unicode_literals

import calendar
import datetime
import logging
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict

import numpy
import pandas as pd
import pytz
from six import string_types

from gtfspy import shapes
from gtfspy.route_types import ALL_ROUTE_TYPES
from gtfspy.route_types import WALK
from gtfspy.util import wgs84_distance

# py2/3 compatibility (copied from six)
if sys.version_info[0] == 3:
    binary_type = bytes
else:
    binary_type = str

if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')

class GTFS(object):

    def __init__(self, fname):
        """Open a GTFS object

        Parameters
        ----------
        fname: str | sqlite3.Connection
            path to the preprocessed gtfs database or a connection to a gtfs database
        """
        if isinstance(fname, string_types):
            if os.path.isfile(fname):
                self.conn = sqlite3.connect(fname)
                self.fname = fname
                # memory-mapped IO size, in bytes
                self.conn.execute('PRAGMA mmap_size = 1000000000;')
                # page cache size, in negative KiB.
                self.conn.execute('PRAGMA cache_size = -2000000;')
            else:
                raise EnvironmentError("File " + fname + " missing")
        elif isinstance(fname, sqlite3.Connection):
            self.conn = fname
            self._dont_close = True
        else:
            raise NotImplementedError(
                "Initiating GTFS using an object with type " + str(type(fname)) + " is not supported")

        assert self.conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchone() is not None
        self.meta = GTFSMetadata(self.conn)
        # Bind functions
        self.conn.create_function("find_distance", 4, wgs84_distance)

        # Set timezones
        self._timezone = pytz.timezone(self.get_timezone_name())

    def __del__(self):
        if not getattr(self, '_dont_close', False):
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
        # this import is here just to avoid circular imports
        from gtfspy.import_gtfs import import_gtfs
        conn = sqlite3.connect(":memory:")
        import_gtfs(gtfs_directory,
                    conn,
                    preserve_connection=True,
                    print_progress=False)
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
        return self.meta.get('location_name', "location_unknown")

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
        query_template = "SELECT max(d) - min(d) " \
                         "FROM shapes JOIN trips ON(trips.shape_id=shapes.shape_id) " \
                         "WHERE trip_I={trip_I} AND shapes.seq>={from_stop_seq} AND shapes.seq<={to_stop_seq};"
        distance_query = query_template.format(trip_I=trip_I, from_stop_seq=from_stop_seq, to_stop_seq=to_stop_seq)
        return self.conn.execute(distance_query).fetchone()[0]


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
        return list(pd.read_sql("SELECT * FROM main.sqlite_master WHERE type='table'", self.conn)["name"])

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
        # TODO!: This is dangerous (?). We should get rid of this IMHO (RK)
        TZ = self.conn.execute('SELECT timezone FROM agencies LIMIT 1').fetchall()[0][0]
        # print TZ
        os.environ['TZ'] = TZ
        time.tzset()  # Cause C-library functions to notice the update.

    def get_timezone_name(self):
        """
        Get name of the GTFS timezone

        Returns
        -------
        timezone_name : str
            name of the time zone, e.g. "Europe/Helsinki"
        """
        tz_name = self.conn.execute('SELECT timezone FROM agencies LIMIT 1'
                                    ).fetchone()
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
            download_date = self.meta.get('download_date')
            if download_date:
                dt = datetime.datetime.strptime(download_date, '%Y-%m-%d')
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

            date = datetime.datetime.strptime(date, '%Y-%m-%d')

        date_noon = datetime.datetime(date.year, date.month, date.day,
                                      12, 0, 0)
        ut_noon = self.unlocalized_datetime_to_ut_seconds(date_noon)
        return ut_noon - 43200  # 43200=12*60*60 (this comes from GTFS: noon-12 hrs)

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
            trip['route_type'] = int(route_type)
            trip['name'] = str(name)

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
                try:
                    shape_breaks.append(int(stop_row.shape_break))
                except TypeError:
                    shape_breaks.append(None)

            if use_shapes:
                # get shape data (from cache, if possible)
                if shape_id not in shape_cache:
                    shape_cache[shape_id] = shapes.get_shape_points2(self.conn.cursor(), shape_id)
                shape_data = shape_cache[shape_id]
                # noinspection PyBroadException
                try:
                    trip['times'] = shapes.interpolate_shape_times(shape_data['d'], shape_breaks, stop_dep_times)
                    trip['lats'] = shape_data['lats']
                    trip['lons'] = shape_data['lons']
                    start_break = shape_breaks[0]
                    end_break = shape_breaks[-1]
                    trip['times'] = trip['times'][start_break:end_break + 1]
                    trip['lats'] = trip['lats'][start_break:end_break + 1]
                    trip['lons'] = trip['lons'][start_break:end_break + 1]
                except:
                    # In case interpolation fails:
                    trip['times'] = stop_dep_times
                    trip['lats'] = stop_lats
                    trip['lons'] = stop_lons
            else:
                trip['times'] = stop_dep_times
                trip['lats'] = stop_lats
                trip['lons'] = stop_lons
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
                if (stop_time_row.dep_time_ut >= start_ut) and (stop_time_row.dep_time_ut <= end_ut):
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
                (stop_I_n, dep_time_ut_n, s_lat_n, s_lon_n, s_seq_n, shape_break_n) = stops_df.iloc[i + 1]
                # test if _contained_ in the interval
                # overlap would read:
                #   (dep_time_ut <= end) and (start <= dep_time_ut_n)
                if (dep_time_ut >= start) and (dep_time_ut_n <= end):
                    seg = (stop_I, stop_I_n)
                    segment_counts[seg] += 1
                    if seg not in seg_to_info:
                        seg_to_info[seg] = {
                            u"trip_I": row.trip_I,
                            u"lats": [s_lat, s_lat_n],
                            u"lons": [s_lon, s_lon_n],
                            u"shape_id": row.shape_id,
                            u"stop_seqs": [s_seq, s_seq_n],
                            u"shape_breaks": [shape_break, shape_break_n]
                        }
                        tripI_to_seq[row.trip_I].append(seg)

        stop_names = {}
        for (stop_I, stop_J) in segment_counts.keys():
            for s in [stop_I, stop_J]:
                if s not in stop_names:
                    stop_names[s] = self.stop(s)[u'name'].values[0]

        seg_data = []
        for seg, count in segment_counts.items():
            segInfo = seg_to_info[seg]
            shape_breaks = segInfo[u"shape_breaks"]
            seg_el = {}
            if use_shapes and shape_breaks and shape_breaks[0] and shape_breaks[1]:
                shape = shapes.get_shape_between_stops(
                    cur,
                    segInfo[u'trip_I'],
                    shape_breaks=shape_breaks
                )
                seg_el[u'lats'] = segInfo[u'lats'][:1] + shape[u'lat'] + segInfo[u'lats'][1:]
                seg_el[u'lons'] = segInfo[u'lons'][:1] + shape[u'lon'] + segInfo[u'lons'][1:]
            else:
                seg_el[u'lats'] = segInfo[u'lats']
                seg_el[u'lons'] = segInfo[u'lons']
            seg_el[u'name'] = stop_names[seg[0]] + u"-" + stop_names[seg[1]]
            seg_el[u'count'] = count
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
        query = "SELECT routes.name as name, shape_id, route_I, trip_I, routes.type, " \
                "        agency_id, agencies.name as agency_name, max(end_time_ds-start_time_ds) as trip_duration " \
                "FROM trips " \
                "LEFT JOIN routes " \
                "USING(route_I) " \
                "LEFT JOIN agencies " \
                "USING(agency_I) " \
                "GROUP BY routes.route_I"
        data = pd.read_sql_query(query, self.conn)
        # print(pd.read_sql_query("select * from agencies", self.conn))
        # print(pd.read_sql_query("select * from routes", self.conn))
        # print(pd.read_sql_query("select * from trips", self.conn))

        routeShapes = []
        n_rows = len(data)
        for i, row in enumerate(data.itertuples()):
            datum = {"name": str(row.name), "type": int(row.type), "agency": str(row.agency_id), "agency_name": str(row.agency_name)}
            # print(row.agency_id, ": ", i, "/", n_rows)
            # this function should be made also non-shape friendly (at this point)
            if use_shapes and row.shape_id:
                shape = shapes.get_shape_points2(cur, row.shape_id)
                lats = shape['lats']
                lons = shape['lons']
            else:
                stop_shape = self.get_trip_stop_coordinates(row.trip_I)
                lats = list(stop_shape['lat'])
                lons = list(stop_shape['lon'])
            datum['lats'] = [float(lat) for lat in lats]
            datum['lons'] = [float(lon) for lon in lons]
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
        query = "SELECT " + to_select + \
                "FROM day_trips " \
                "WHERE " \
                "(end_time_ut > {start_ut} AND start_time_ut < {end_ut})".format(start_ut=start, end_ut=end)
        return pd.read_sql_query(query, self.conn)

    def get_trip_counts_per_day(self):
        """
        Get trip counts per day between the start and end day of hte feed.

        Returns
        -------
        trip_counts : pandas.DataFrame
            has columns "dates" and "trip_counts" where
                dates are strings
                trip_counts are ints
        """
        query = "SELECT date, count(*) AS number_of_trips FROM day_trips GROUP BY date"
        # this yields the actual data
        trip_counts_per_day = pd.read_sql_query(query, self.conn, index_col="date")
        # the rest is simply code for filling out "gaps" in the time span
        # (necessary for some visualizations)
        max_day = trip_counts_per_day.index.max()
        min_day = trip_counts_per_day.index.min()
        min_date = datetime.datetime.strptime(min_day, '%Y-%m-%d')
        max_date = datetime.datetime.strptime(max_day, '%Y-%m-%d')
        num_days = (max_date - min_date).days
        dates = [min_date + datetime.timedelta(days=x) for x in range(num_days + 1)]
        trip_counts = []
        date_strings = []
        for date in dates:
            date_string = date.strftime("%Y-%m-%d")
            date_strings.append(date_string)
            try:
                value = trip_counts_per_day.loc[date_string, 'number_of_trips']
            except KeyError:
                # set value to 0 if dsut is not present, i.e. when no trips
                # take place on that day
                value = 0
            trip_counts.append(value)
        # check that all date_strings are included (move this to tests?)
        for date_string in trip_counts_per_day.index:
            assert date_string in date_strings
        data = {"dates": date_strings, "trip_counts": trip_counts}
        return pd.DataFrame(data)

        # Remove these pieces of code when this function has been tested:
        #
        # (RK) not sure if this works or not:
        # def localized_datetime_to_ut_seconds(self, loc_dt):
        #     utcoffset = loc_dt.utcoffset()
        #     print utcoffset
        #     utc_naive  = loc_dt.replace(tzinfo=None) - utcoffset
        #     timestamp = (utc_naive - datetime.datetime(1970, 1, 1)).total_seconds()
        #     return timestamp

        # def
        # query = "SELECT day_start_ut, count(*) AS number_of_trips FROM day_trips GROUP BY day_start_ut"
        # trip_counts_per_day = pd.read_sql_query(query, self.conn, index_col="day_start_ut")
        # min_day_start_ut = trip_counts_per_day.index.min()
        # max_day_start_ut = trip_counts_per_day.index.max()
        # spacing = 24*3600
        # # day_start_ut is noon - 12 hours (to cover for daylight saving time changes)
        # min_date_noon = self.ut_seconds_to_gtfs_datetime(min_day_start_ut)+datetime.timedelta(hours=12)
        # max_date_noon = self.ut_seconds_to_gtfs_datetime(max_day_start_ut)+datetime.timedelta(hours=12)
        # num_days = (max_date_noon-min_date_noon).days
        # print min_date_noon, max_date_noon
        # dates_noon = [min_date_noon + datetime.timedelta(days=x) for x in range(0, num_days+1)]
        # day_noon_uts = [int(self.localized_datetime_to_ut_seconds(date)) for date in dates_noon]
        # day_start_uts = [dnu-12*3600 for dnu in day_noon_uts]
        # print day_start_uts
        # print list(trip_counts_per_day.index.values)

        # assert max_day_start_ut == day_start_uts[-1]
        # assert min_day_start_ut == day_start_uts[0]

        # trip_counts = []
        # for dsut in day_start_uts:
        #     try:
        #         value = trip_counts_per_day.loc[dsut, 'number_of_trips']
        #     except KeyError as e:
        #         # set value to 0 if dsut is not present, i.e. when no trips
        #         # take place on that day
        #         value = 0
        #     trip_counts.append(value)
        # for dsut in trip_counts_per_day.index:
        #     assert dsut in day_start_uts
        # return {"day_start_uts": day_start_uts, "trip_counts":trip_counts}

    def get_suitable_date_for_daily_extract(self, date=None, ut=False):
        '''
        Selects suitable date for daily extract
        Iterates trough the available dates forward and backward from the download date accepting the first day that has
        at least 90 percent of the number of trips of the maximum date. The condition can be changed to something else.
        If the download date is out of range, the process will look trough the dates from first to last.
        :param daily_trips: pandas dataframe
        :param date: date string
        :return:
        '''
        daily_trips = self.get_trip_counts_per_day()
        max_daily_trips = daily_trips[u'trip_counts'].max(axis=0)
        if date in daily_trips[u'dates']:
            start_index = daily_trips[daily_trips[u'dates'] == date].index.tolist()[0]
            daily_trips[u'old_index'] = daily_trips.index
            daily_trips[u'date_dist'] = abs(start_index - daily_trips.index)
            daily_trips = daily_trips.sort_values(by=[u'date_dist', u'old_index']).reindex()
        for row in daily_trips.itertuples():
            if row.trip_counts >= 0.9 * max_daily_trips:
                if ut:
                    return self.get_day_start_ut(row.dates)
                else:
                    return row.dates

    def get_spreading_trips(self, start_time_ut, lat, lon,
                            max_duration_ut=4 * 3600,
                            min_transfer_time=30,
                            use_shapes=False):
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
        spreader = Spreader(self, start_time_ut, lat, lon, max_duration_ut, min_transfer_time, use_shapes)
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
        results = cur.execute("SELECT name, type FROM routes JOIN trips USING(route_I) WHERE trip_I={trip_I}"
                              .format(trip_I=trip_I))
        name, rtype = results.fetchone()
        return u"%s" % str(name), int(rtype)

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
        return unicode(name), int(rtype)

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
                    ORDER BY stop_times.seq""".format(trip_I=trip_I)
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
        to_select = "stop_I, " + str(day_start_ut) + "+dep_time_ds AS dep_time_ut, lat, lon, seq, shape_break"
        str_to_run = "SELECT " + to_select + """
                        FROM stop_times JOIN stops USING(stop_I)
                        WHERE (trip_I ={trip_I}) ORDER BY seq
                      """
        str_to_run = str_to_run.format(trip_I=trip_I)
        return pd.read_sql_query(str_to_run, self.conn)

    def get_events_by_tripI_and_dsut(self, trip_I, day_start_ut,
                                     start_ut=None, end_ut=None):
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
        params = [day_start_ut, day_start_ut,
                  trip_I]
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
                "arr_time_ut": stop_data[i + 1][1]
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
            assert len(rows) == 1, 'On a day, a trip_I should be present at most once'
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
        self.set_current_process_time_zone()
        # last -1 equals to 'not known' for DST (automatically deduced then)
        return time.mktime(time.localtime(ut)[:3] + (12, 00, 0, 0, 0, -1)) - 43200

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
        self.set_current_process_time_zone()
        day0 = time.localtime(day_start_ut + 43200)  # time of noon
        dayN = time.mktime(day0[:2] +  # YYYY, MM
                           (day0[2] + n_days,) +  # DD
                           (12, 00, 0, 0, 0, -1)) - 43200  # HHMM, etc.  Minus 12 hours.
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

    def get_tripIs_within_range_by_dsut(self,
                                        start_time_ut,
                                        end_time_ut):
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
        dst_ut, st_ds, et_ds = \
            self._get_possible_day_starts(start_time_ut, end_time_ut, 7)
        # noinspection PyTypeChecker
        assert len(dst_ut) >= 0
        trip_I_dict = {}
        for day_start_ut, start_ds, end_ds in \
                zip(dst_ut, st_ds, et_ds):
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
        return pd.read_sql_query("SELECT * FROM stops WHERE stop_I={stop_I}".format(stop_I=stop_I), self.conn)

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
            return pd.read_sql_query("SELECT DISTINCT stops.stop_I, stops.* "
                                     "FROM stops JOIN stop_times ON stops.stop_I == stop_times.stop_I "
                                     "           JOIN trips ON stop_times.trip_I = trips.trip_I"
                                     "           JOIN routes ON trips.route_I == routes.route_I "
                                     "WHERE routes.type=(?)", self.conn, params=(route_type,))

    def generate_routable_transit_events(self, start_time_ut=None, end_time_ut=None, route_type=None):
        """
        Generates events that take place during a time interval.
        Each event needs to be only partially overlap the given time interval.
        Does not include walking events. This is just a quick and dirty implementation to get a way of quickly get a
        method for generating events compatible with the routing algorithm
        :param start_time_ut:
        :param end_time_ut:
        :param route_type:
        :return: generates named tuples of the events
                dep_time_ut: int
                arr_time_ut: int
                from_stop_I: int
                to_stop_I: int
                trip_I : int
                route_type : int
                seq: int

        """
        from gtfspy.networks import temporal_network
        df = temporal_network(self, start_time_ut=start_time_ut, end_time_ut=end_time_ut, route_type=route_type)
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
        event_query = "SELECT stop_I, seq, trip_I, route_I, routes.route_id AS route_id, routes.type AS route_type, " \
                          "shape_id, day_start_ut+dep_time_ds AS dep_time_ut, day_start_ut+arr_time_ds AS arr_time_ut " \
                      "FROM " + table_name + " " \
                      "JOIN trips USING(trip_I) " \
                      "JOIN routes USING(route_I) " \
                      "JOIN stop_times USING(trip_I)"

        where_clauses = []
        if end_time_ut:
            where_clauses.append(table_name + ".start_time_ut< {end_time_ut}".format(end_time_ut=end_time_ut))
            where_clauses.append("dep_time_ut  <={end_time_ut}".format(end_time_ut=end_time_ut))
        if start_time_ut:
            where_clauses.append(table_name + ".end_time_ut  > {start_time_ut}".format(start_time_ut=start_time_ut))
            where_clauses.append("arr_time_ut  >={start_time_ut}".format(start_time_ut=start_time_ut))
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
            (events_result['trip_I'][:-1].values == events_result['trip_I'][1:].values) *
            (events_result['seq'][:-1].values < events_result['seq'][1:].values)
        )[0]
        to_indices = from_indices + 1
        # these should have same trip_ids
        assert (events_result['trip_I'][from_indices] == events_result['trip_I'][to_indices]).all()
        trip_Is = events_result['trip_I'][from_indices]
        from_stops = events_result['stop_I'][from_indices]
        to_stops = events_result['stop_I'][to_indices]
        shape_ids = events_result['shape_id'][from_indices]
        dep_times = events_result['dep_time_ut'][from_indices]
        arr_times = events_result['arr_time_ut'][to_indices]
        route_types = events_result['route_type'][from_indices]
        route_ids = events_result['route_id'][from_indices]
        durations = arr_times.values - dep_times.values
        assert (durations >= 0).all()
        from_seqs = events_result['seq'][from_indices]
        to_seqs = events_result['seq'][to_indices]
        data_tuples = zip(from_stops, to_stops, dep_times, arr_times,
                          shape_ids, route_types, route_ids, trip_Is,
                          durations, from_seqs, to_seqs)
        columns = ["from_stop_I", "to_stop_I", "dep_time_ut", "arr_time_ut",
                   "shape_id", "route_type", "route_id", "trip_I",
                   "duration", "from_seq", "to_seq"]
        df = pd.DataFrame.from_records(data_tuples, columns=columns)
        return df

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
            query = u""" SELECT from_stop_I, to_stop_I, d
                        FROM stop_distances
                            WHERE
                                from_stop_I=?
                    """
            params = (u"{stop_I}".format(stop_I=stop_I),)
        else:
            query = """ SELECT from_stop_I, to_stop_I, d
                        FROM stop_distances
                    """
            params = None
        stop_data_df = pd.read_sql_query(query, self.conn, params=params)
        return stop_data_df

    def update_stats(self, stats):
        self.meta.update(stats)
        self.meta['stats_calc_at_ut'] = time.time()

    def get_conservative_gtfs_time_span_in_ut(self):
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
        first_day_start_ut, last_day_start_ut = \
            cur.execute("SELECT min(day_start_ut), max(day_start_ut) FROM days;").fetchone()
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
        return validator.get_warnings()

    def execute_custom_query(self, query):
        return self.conn.cursor().execute(query)

    def execute_custom_query_pandas(self, query):
        return pd.read_sql(query, self.conn)

    def get_stats(self):
        from gtfspy import stats
        return stats.get_stats(self)

    def _get_day_trips_table_name(self):
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='day_trips2'")
        if len(cur.fetchall()) > 0:
            table_name = "day_trips2"
        else:
            table_name = "day_trips"
        return table_name


class GTFSMetadata(object):
    """
    This provides dictionary protocol for updating GTFS metadata ("meta table").

    TODO: does not rep ???
    """

    def __init__(self, conn):
        self._conn = conn

    def __getitem__(self, key):
        val = self._conn.execute('SELECT value FROM metadata WHERE key=?',
                                 (key,)).fetchone()
        if not val:
            raise KeyError("This GTFS does not have metadata: %s" % key)
        return val[0]

    def __setitem__(self, key, value):
        """Get metadata from the DB"""
        if isinstance(value, binary_type):
            value = value.decode('utf-8')
        self._conn.execute('INSERT OR REPLACE INTO metadata '
                           '(key, value) VALUES (?, ?)',
                           (key, value)).fetchone()
        self._conn.commit()

    def __delitem__(self, key):
        self._conn.execute('DELETE FROM metadata WHERE key=?',
                           (key,)).fetchone()
        self._conn.commit()

    def __iter__(self):
        cur = self._conn.execute('SELECT key FROM metadata ORDER BY key')
        return (x[0] for x in cur)

    def __contains__(self, key):
        val = self._conn.execute('SELECT value FROM metadata WHERE key=?',
                                 (key,)).fetchone()
        return val is not None

    def get(self, key, default=None):
        val = self._conn.execute('SELECT value FROM metadata WHERE key=?',
                                 (key,)).fetchone()
        if not val:
            return default
        return val[0]

    def items(self):
        cur = self._conn.execute('SELECT key, value FROM metadata ORDER BY key')
        return cur

    def update(self, dict_):
        # Would be more efficient to do it in a new query here, but
        # preferring simplicity.  metadata updates are probably
        # infrequent.
        if hasattr(dict_, 'items'):
            for key, value in dict_.items():
                self[key] = value
        else:
            for key, value in dict_:
                self[key] = value


def main(cmd, args):
    from gtfspy import filter
    # noinspection PyPackageRequirements
    if cmd == 'stats':
        print(args[0])
        G = GTFS(args[0])
        stats = G.get_stats()
        G.update_stats(stats)
        for row in G.meta.items():
            print(row)
    elif cmd == "validate":
        G = GTFS(args[0])
        G.print_validation_warnings()
    elif cmd == 'metadata-list':
        # print args[0]  # need to not print to be valid json on stdout
        G = GTFS(args[0])
        # for row in G.meta.items():
        #    print row
        stats = dict(G.meta.items())
        import json
        print(json.dumps(stats, sort_keys=True,
                         indent=4, separators=(',', ': ')))
    elif cmd == 'make-daily':
        from_db = args[0]
        g = GTFS(from_db)
        to_db = args[1]
        download_date = g.meta['download_date']
        d = datetime.datetime.strptime(download_date, '%Y-%m-%d').date()
        start_time = d + datetime.timedelta(7 - d.isoweekday() + 1)      # inclusive
        end_time   = d + datetime.timedelta(7 - d.isoweekday() + 1 + 1)  # exclusive
        filter.filter_extract(g, to_db, start_date=start_time, end_date=end_time)
    elif cmd == 'make-weekly':
        from_db = args[0]
        g = GTFS(from_db)
        to_db = args[1]
        download_date = g.meta['download_date']
        d = datetime.datetime.strptime(download_date, '%Y-%m-%d').date()
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
            print("spatial-extract usage: python gtfs.py spatial-extract fromdb.sqlite center_lat center_lon "
                  "radius_in_km todb.sqlite")
            raise e
        logging.basicConfig(level=logging.INFO)
        logging.info("Loading initial database")
        g = GTFS(from_db)
        filter.filter_extract(g, to_db, buffer_distance=radius_in_km * 1000, buffer_lat=lat, buffer_lon=lon)
    elif cmd == 'interact':
        # noinspection PyUnusedLocal
        G = GTFS(args[0])
        # noinspection PyPackageRequirements
        import IPython
        IPython.embed()
    elif 'export_shapefile' in cmd:
        from gtfspy.util import write_shapefile
        from_db = args[0] #'/m/cs/project/networks/jweckstr/transit/scratch/proc_latest/helsinki/2016-04-06/main.day.sqlite'
        shapefile_path = args[1] #'/m/cs/project/networks/jweckstr/TESTDATA/helsinki_routes.shp'
        g = GTFS(from_db)
        if cmd == 'export_shapefile_routes':
            data = g.get_all_route_shapes(use_shapes=True)

        elif cmd == 'export_shapefile_segment_counts':
            date = args[2] # '2016-04-06'
            d = datetime.datetime.strptime(date, '%Y-%m-%d').date()
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


