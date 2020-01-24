from __future__ import unicode_literals

import csv
import os
import sys

import numpy
import pandas as pd

from gtfspy.gtfs import GTFS
from gtfspy.util import wgs84_distance


def get_spatial_bounds(gtfs, as_dict=False):
    """
    Parameters
    ----------
    gtfs

    Returns
    -------
    min_lon: float
    max_lon: float
    min_lat: float
    max_lat: float
    """
    stats = get_stats(gtfs)
    lon_min = stats["lon_min"]
    lon_max = stats["lon_max"]
    lat_min = stats["lat_min"]
    lat_max = stats["lat_max"]
    if as_dict:
        return {"lon_min": lon_min, "lon_max": lon_max, "lat_min": lat_min, "lat_max": lat_max}
    else:
        return lon_min, lon_max, lat_min, lat_max


def get_percentile_stop_bounds(gtfs, percentile):
    stops = gtfs.get_table("stops")
    percentile = min(percentile, 100 - percentile)
    lat_min = numpy.percentile(stops["lat"].values, percentile)
    lat_max = numpy.percentile(stops["lat"].values, 100 - percentile)
    lon_min = numpy.percentile(stops["lon"].values, percentile)
    lon_max = numpy.percentile(stops["lon"].values, 100 - percentile)
    return lon_min, lon_max, lat_min, lat_max


def get_median_lat_lon_of_stops(gtfs):
    """
    Get median latitude AND longitude of stops

    Parameters
    ----------
    gtfs: GTFS

    Returns
    -------
    median_lat : float
    median_lon : float
    """
    stops = gtfs.get_table("stops")
    median_lat = numpy.percentile(stops["lat"].values, 50)
    median_lon = numpy.percentile(stops["lon"].values, 50)
    return median_lat, median_lon


def get_centroid_of_stops(gtfs):
    """
    Get mean latitude AND longitude of stops

    Parameters
    ----------
    gtfs: GTFS

    Returns
    -------
    mean_lat : float
    mean_lon : float
    """
    stops = gtfs.get_table("stops")
    mean_lat = numpy.mean(stops["lat"].values)
    mean_lon = numpy.mean(stops["lon"].values)
    return mean_lat, mean_lon


def write_stats_as_csv(gtfs, path_to_csv, re_write=False):
    """
    Writes data from get_stats to csv file

    Parameters
    ----------
    gtfs: GTFS
    path_to_csv: str
        filepath to the csv file to be generated
    re_write:
        insted of appending, create a new one.
    """
    stats_dict = get_stats(gtfs)
    # check if file exist
    if re_write:
        os.remove(path_to_csv)

    # if not os.path.isfile(path_to_csv):
    #   is_new = True
    # else:
    #   is_new = False

    is_new = True
    mode = "r" if os.path.exists(path_to_csv) else "w+"
    with open(path_to_csv, mode) as csvfile:
        for line in csvfile:
            if line:
                is_new = False
            else:
                is_new = True

    with open(path_to_csv, "a") as csvfile:
        if sys.version_info > (3, 0):
            delimiter = ","
        else:
            delimiter = b","
        statswriter = csv.writer(csvfile, delimiter=delimiter)
        # write column names if
        if is_new:
            statswriter.writerow([key for key in sorted(stats_dict.keys())])

        row_to_write = []
        # write stats row sorted by column name
        for key in sorted(stats_dict.keys()):
            row_to_write.append(stats_dict[key])
        statswriter.writerow(row_to_write)


def get_stats(gtfs):
    """
    Get basic statistics of the GTFS data.

    Parameters
    ----------
    gtfs: GTFS

    Returns
    -------
    stats: dict
        A dictionary of various statistics.
        Keys should be strings, values should be inputtable to a database (int, date, str, ...)
        (but not a list)
    """
    stats = {}
    # Basic table counts
    for table in [
        "agencies",
        "routes",
        "stops",
        "stop_times",
        "trips",
        "calendar",
        "shapes",
        "calendar_dates",
        "days",
        "stop_distances",
        "frequencies",
        "feed_info",
        "transfers",
    ]:
        stats["n_" + table] = gtfs.get_row_count(table)

    # Agency names
    agencies = gtfs.get_table("agencies")
    stats["agencies"] = "_".join(agencies["name"].values)

    # Stop lat/lon range
    stops = gtfs.get_table("stops")
    lats = stops["lat"].values
    lons = stops["lon"].values
    percentiles = [0, 10, 50, 90, 100]

    try:
        lat_percentiles = numpy.percentile(lats, percentiles)
    except IndexError:
        lat_percentiles = [None] * 5
    lat_min, lat_10, lat_median, lat_90, lat_max = lat_percentiles
    stats["lat_min"] = lat_min
    stats["lat_10"] = lat_10
    stats["lat_median"] = lat_median
    stats["lat_90"] = lat_90
    stats["lat_max"] = lat_max

    try:
        lon_percentiles = numpy.percentile(lons, percentiles)
    except IndexError:
        lon_percentiles = [None] * 5
    lon_min, lon_10, lon_median, lon_90, lon_max = lon_percentiles
    stats["lon_min"] = lon_min
    stats["lon_10"] = lon_10
    stats["lon_median"] = lon_median
    stats["lon_90"] = lon_90
    stats["lon_max"] = lon_max

    if len(lats) > 0:
        stats["height_km"] = wgs84_distance(lat_min, lon_median, lat_max, lon_median) / 1000.0
        stats["width_km"] = wgs84_distance(lon_min, lat_median, lon_max, lat_median) / 1000.0
    else:
        stats["height_km"] = None
        stats["width_km"] = None

    first_day_start_ut, last_day_start_ut = gtfs.get_day_start_ut_span()
    stats["start_time_ut"] = first_day_start_ut
    if last_day_start_ut is None:
        stats["end_time_ut"] = None
    else:
        # 28 (instead of 24) comes from the GTFS stANDard
        stats["end_time_ut"] = last_day_start_ut + 28 * 3600

    stats["start_date"] = gtfs.get_min_date()
    stats["end_date"] = gtfs.get_max_date()

    # Maximum activity day
    max_activity_date = gtfs.execute_custom_query(
        "SELECT count(*), date "
        "FROM days "
        "GROUP BY date "
        "ORDER BY count(*) DESC, date "
        "LIMIT 1;"
    ).fetchone()
    if max_activity_date:
        stats["max_activity_date"] = max_activity_date[1]
        max_activity_hour = (
            gtfs.get_cursor()
            .execute(
                "SELECT count(*), arr_time_hour FROM day_stop_times "
                "WHERE date=? GROUP BY arr_time_hour "
                "ORDER BY count(*) DESC;",
                (stats["max_activity_date"],),
            )
            .fetchone()
        )
        if max_activity_hour:
            stats["max_activity_hour"] = max_activity_hour[1]
        else:
            stats["max_activity_hour"] = None

    # Fleet size estimate: considering each line separately
    if max_activity_date and max_activity_hour:
        fleet_size_estimates = _fleet_size_estimate(
            gtfs, stats["max_activity_hour"], stats["max_activity_date"]
        )
        stats.update(fleet_size_estimates)

    # Compute simple distributions of various columns that have a finite range of values.
    # Commented lines refer to values that are not imported yet, ?

    stats["routes__type__dist"] = _distribution(gtfs, "routes", "type")
    # stats['stop_times__pickup_type__dist'] = _distribution(gtfs, 'stop_times', 'pickup_type')
    # stats['stop_times__drop_off_type__dist'] = _distribution(gtfs, 'stop_times', 'drop_off_type')
    # stats['stop_times__timepoint__dist'] = _distribution(gtfs, 'stop_times', 'timepoint')
    stats["calendar_dates__exception_type__dist"] = _distribution(
        gtfs, "calendar_dates", "exception_type"
    )
    stats["frequencies__exact_times__dist"] = _distribution(gtfs, "frequencies", "exact_times")
    stats["transfers__transfer_type__dist"] = _distribution(gtfs, "transfers", "transfer_type")
    stats["agencies__lang__dist"] = _distribution(gtfs, "agencies", "lang")
    stats["stops__location_type__dist"] = _distribution(gtfs, "stops", "location_type")
    # stats['stops__wheelchair_boarding__dist'] = _distribution(gtfs, 'stops', 'wheelchair_boarding')
    # stats['trips__wheelchair_accessible__dist'] = _distribution(gtfs, 'trips', 'wheelchair_accessible')
    # stats['trips__bikes_allowed__dist'] = _distribution(gtfs, 'trips', 'bikes_allowed')
    # stats[''] = _distribution(gtfs, '', '')
    stats = _feed_calendar_span(gtfs, stats)

    return stats


def _distribution(gtfs, table, column):
    """Count occurrences of values AND return it as a string.

    Example return value:   '1:5 2:15'"""
    cur = gtfs.conn.cursor()
    cur.execute(
        "SELECT {column}, count(*) "
        "FROM {table} GROUP BY {column} "
        "ORDER BY {column}".format(column=column, table=table)
    )
    return " ".join("%s:%s" % (t, c) for t, c in cur)


def _fleet_size_estimate(gtfs, hour, date):
    """
    Calculates fleet size estimates by two separate formula:
     1. Considering all routes separately with no interlining and doing a deficit calculation at every terminal
     2. By looking at the maximum number of vehicles in simultaneous movement

    Parameters
    ----------
    gtfs: GTFS
    hour: int
    date: ?

    Returns
    -------
    results: dict
        a dict with keys:
            fleet_size_route_based
            fleet_size_max_movement

    """
    results = {}

    fleet_size_list = []
    cur = gtfs.conn.cursor()
    rows = cur.execute(
        "SELECT type, max(vehicles) "
        "FROM ("
        "SELECT type, direction_id, sum(vehicles) as vehicles "
        "FROM "
        "("
        "SELECT trips.route_I, trips.direction_id, routes.route_id, name, type, count(*) as vehicles, cycle_time_min "
        "FROM trips, routes, days, "
        "("
        "SELECT first_trip.route_I, first_trip.direction_id, first_trip_start_time, first_trip_end_time, "
        "MIN(start_time_ds) as return_trip_start_time, end_time_ds as return_trip_end_time, "
        "(end_time_ds - first_trip_start_time)/60 as cycle_time_min "
        "FROM "
        "trips, "
        "(SELECT route_I, direction_id, MIN(start_time_ds) as first_trip_start_time, "
        "end_time_ds as first_trip_end_time "
        "FROM trips, days "
        "WHERE trips.trip_I=days.trip_I AND start_time_ds >= ? * 3600 "
        "AND start_time_ds <= (? + 1) * 3600 AND date = ? "
        "GROUP BY route_I, direction_id) first_trip "
        "WHERE first_trip.route_I = trips.route_I "
        "AND first_trip.direction_id != trips.direction_id "
        "AND start_time_ds >= first_trip_end_time "
        "GROUP BY trips.route_I, trips.direction_id"
        ") return_trip "
        "WHERE trips.trip_I=days.trip_I AND trips.route_I= routes.route_I "
        "AND date = ? AND trips.route_I = return_trip.route_I "
        "AND trips.direction_id = return_trip.direction_id "
        "AND start_time_ds >= first_trip_start_time "
        "AND start_time_ds < return_trip_end_time "
        "GROUP BY trips.route_I, trips.direction_id "
        "ORDER BY type, name, vehicles desc"
        ") cycle_times "
        "GROUP BY direction_id, type"
        ") vehicles_type "
        "GROUP BY type;",
        (hour, hour, date, date),
    )
    for row in rows:
        fleet_size_list.append(str(row[0]) + ":" + str(row[1]))
    results["fleet_size_route_based"] = " ".join(fleet_size_list)

    # Fleet size estimate: maximum number of vehicles in movement
    fleet_size_list = []
    fleet_size_dict = {}
    if hour:
        for minute in range(hour * 3600, (hour + 1) * 3600, 60):
            rows = gtfs.conn.cursor().execute(
                "SELECT type, count(*) "
                "FROM trips, routes, days "
                "WHERE trips.route_I = routes.route_I "
                "AND trips.trip_I=days.trip_I "
                "AND start_time_ds <= ? "
                "AND end_time_ds > ? + 60 "
                "AND date = ? "
                "GROUP BY type;",
                (minute, minute, date),
            )

            for row in rows:
                if fleet_size_dict.get(row[0], 0) < row[1]:
                    fleet_size_dict[row[0]] = row[1]

    for key in fleet_size_dict.keys():
        fleet_size_list.append(str(key) + ":" + str(fleet_size_dict[key]))
    results["fleet_size_max_movement"] = " ".join(fleet_size_list)
    return results


def _n_gtfs_sources(gtfs):
    n_gtfs_sources = gtfs.execute_custom_query(
        "SELECT value FROM metadata WHERE key = 'n_gtfs_sources';"
    ).fetchone()
    if not n_gtfs_sources:
        n_gtfs_sources = [1]
    return n_gtfs_sources


def _feed_calendar_span(gtfs, stats):
    """
    Computes the temporal coverage of each source feed

    Parameters
    ----------
    gtfs: gtfspy.GTFS object
    stats: dict
        where to append the stats

    Returns
    -------
    stats: dict
    """
    n_feeds = _n_gtfs_sources(gtfs)[0]
    max_start = None
    min_end = None
    if n_feeds > 1:
        for i in range(n_feeds):
            feed_key = "feed_" + str(i) + "_"
            start_key = feed_key + "calendar_start"
            end_key = feed_key + "calendar_end"
            calendar_span = (
                gtfs.conn.cursor()
                .execute(
                    "SELECT min(date), max(date) FROM trips, days "
                    "WHERE trips.trip_I = days.trip_I AND trip_id LIKE ?;",
                    (feed_key + "%",),
                )
                .fetchone()
            )

            stats[start_key] = calendar_span[0]
            stats[end_key] = calendar_span[1]
            if calendar_span[0] is not None and calendar_span[1] is not None:
                if not max_start and not min_end:
                    max_start = calendar_span[0]
                    min_end = calendar_span[1]
                else:
                    if gtfs.get_day_start_ut(calendar_span[0]) > gtfs.get_day_start_ut(max_start):
                        max_start = calendar_span[0]
                    if gtfs.get_day_start_ut(calendar_span[1]) < gtfs.get_day_start_ut(min_end):
                        min_end = calendar_span[1]
        stats["latest_feed_start_date"] = max_start
        stats["earliest_feed_end_date"] = min_end
    else:
        stats["latest_feed_start_date"] = stats["start_date"]
        stats["earliest_feed_end_date"] = stats["end_date"]
    return stats


def update_stats(gtfs):
    """
    Computes stats AND stores them into the underlying gtfs object (i.e. database).

    Parameters
    ----------
    gtfs: GTFS
    """
    stats = get_stats(gtfs)
    gtfs.update_stats(stats)


def trip_stats(gtfs, results_by_mode=False):
    """

    Parameters
    ----------
    gtfs: GTFS
    results_by_mode: bool

    Returns
    -------
    if results_by_mode is False:
        q_result: pandas.DataFrame
    if results_by_mode is True:
        q_results: dict
            a dict with the following keys:
                [ADD HERE]
    """
    conn = gtfs.conn

    conn.create_function("find_distance", 4, wgs84_distance)
    # this query calculates the distance and travel time for each complete trip
    # stop_data_df = pd.read_sql_query(query, self.conn, params=params)

    query = (
        "SELECT "
        "startstop.trip_I AS trip_I, "
        "type, "
        "sum(CAST(find_distance(startstop.lat, startstop.lon, endstop.lat, endstop.lon) AS INT)) as total_distance, "
        "sum(endstop.arr_time_ds - startstop.arr_time_ds) as total_traveltime "
        "FROM "
        "(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) startstop, "
        "(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) endstop, "
        "trips, "
        "routes "
        "WHERE "
        "startstop.trip_I = endstop.trip_I "
        "AND startstop.seq + 1 = endstop.seq "
        "AND startstop.trip_I = trips.trip_I "
        "AND trips.route_I = routes.route_I "
        "GROUP BY startstop.trip_I"
    )

    q_result = pd.read_sql_query(query, conn)
    q_result["avg_speed_kmh"] = 3.6 * q_result["total_distance"] / q_result["total_traveltime"]
    q_result["total_distance"] = q_result["total_distance"] / 1000
    q_result["total_traveltime"] = q_result["total_traveltime"] / 60
    q_result = q_result.loc[q_result["avg_speed_kmh"] != float("inf")]

    if results_by_mode:
        q_results = {}
        for type in q_result["type"].unique().tolist():
            q_results[type] = q_result.loc[q_result["type"] == type]
        return q_results
    else:
        return q_result


def get_section_stats(gtfs, results_by_mode=False):
    conn = gtfs.conn

    conn.create_function("find_distance", 4, wgs84_distance)
    # this query calculates the distance and travel time for each stop to stop section for each trip
    # stop_data_df = pd.read_sql_query(query, self.conn, params=params)

    query = (
        "SELECT type, from_stop_I, to_stop_I, distance, min(travel_time) AS min_time, max(travel_time) AS max_time, avg(travel_time) AS mean_time "
        "FROM "
        "(SELECT q1.trip_I, type, q1.stop_I as from_stop_I, q2.stop_I as to_stop_I,  "
        "CAST(find_distance(q1.lat, q1.lon, q2.lat, q2.lon) AS INT) as distance, "
        "q2.arr_time_ds - q1.arr_time_ds as travel_time, "
        "q1.lat AS from_lat, q1.lon AS from_lon, q2.lat AS to_lat, q2.lon AS to_lon "
        "FROM "
        "(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) q1, "
        "(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) q2, "
        "trips, "
        "routes "
        "WHERE q1.trip_I = q2.trip_I "
        "AND q1.seq + 1 = q2.seq "
        "AND q1.trip_I = trips.trip_I "
        "AND trips.route_I = routes.route_I) sq1 "
        "GROUP BY to_stop_I, from_stop_I, type "
    )

    q_result = pd.read_sql_query(query, conn)

    if results_by_mode:
        q_results = {}
        for type in q_result["type"].unique().tolist():
            q_results[type] = q_result.loc[q_result["type"] == type]
        return q_results
    else:
        return q_result


def route_frequencies(gtfs, results_by_mode=False):
    """
    Return the frequency of all types of routes per day.

    Parameters
    -----------
    gtfs: GTFS

    Returns
    -------
    pandas.DataFrame with columns
        route_I, type, frequency
    """
    day = gtfs.get_suitable_date_for_daily_extract()
    query = (
        " SELECT f.route_I, type, frequency FROM routes as r"
        " JOIN"
        " (SELECT route_I, COUNT(route_I) as frequency"
        " FROM"
        " (SELECT date, route_I, trip_I"
        " FROM day_stop_times"
        " WHERE date = '{day}'"
        " GROUP by route_I, trip_I)"
        " GROUP BY route_I) as f"
        " ON f.route_I = r.route_I"
        " ORDER BY frequency DESC".format(day=day)
    )

    return pd.DataFrame(gtfs.execute_custom_query_pandas(query))


def hourly_frequencies(gtfs, st, et, route_type):
    """
    Return all the number of vehicles (i.e. busses,trams,etc) that pass hourly through a stop in a time frame.

    Parameters
    ----------
    gtfs: GTFS
    st : int
        start time of the time framein unix time
    et : int
        end time of the time frame in unix time
    route_type: int

    Returns
    -------
    numeric pandas.DataFrame with columns
        stop_I, lat, lon, frequency
    """
    timeframe = et - st
    hours = timeframe / 3600
    day = gtfs.get_suitable_date_for_daily_extract()
    stops = gtfs.get_stops_for_route_type(route_type).T.drop_duplicates().T
    query = (
        "SELECT * FROM stops as x"
        " JOIN"
        " (SELECT * , COUNT(*)/{h} as frequency"
        " FROM stop_times, days"
        " WHERE stop_times.trip_I = days.trip_I"
        " AND dep_time_ds > {st}"
        " AND dep_time_ds < {et}"
        " AND date = '{day}'"
        " GROUP BY stop_I) as y"
        " ON y.stop_I = x.stop_I".format(h=hours, st=st, et=et, day=day)
    )
    try:
        trips_frequency = gtfs.execute_custom_query_pandas(query).T.drop_duplicates().T
        df = pd.merge(
            stops[["stop_I", "lat", "lon"]],
            trips_frequency[["stop_I", "frequency"]],
            on="stop_I",
            how="inner",
        )
        return df.apply(pd.to_numeric)
    except:
        raise ValueError("Maybe too short time frame!")


def frequencies_by_generated_route(gtfs, st, et, day=None):
    timeframe = et - st
    hours = timeframe / 3600
    if not day:
        day = gtfs.get_suitable_date_for_daily_extract()
    query = """SELECT count(*)/{h} AS frequency, count(*) AS n_trips, route, type FROM
    (SELECT trip_I, group_concat(stop_I) AS route, name, type FROM
    (SELECT * FROM stop_times, days, trips, routes
    WHERE stop_times.trip_I = days.trip_I AND stop_times.trip_I = trips.trip_I AND  routes.route_I = trips.route_I AND
    days.date = '{day}' AND start_time_ds >= {st} AND start_time_ds < {et}
    ORDER BY trip_I, seq) q1
    GROUP BY trip_I) q2
    GROUP BY route""".format(
        h=hours, st=st, et=et, day=day
    )
    df = gtfs.execute_custom_query_pandas(query)
    return df


def departure_stops(gtfs, st, et):
    day = gtfs.get_suitable_date_for_daily_extract()
    query = """select stop_I, count(*) as n_departures from
                (select min(seq), * from stop_times, days, trips
                where stop_times.trip_I = days.trip_I and stop_times.trip_I = trips.trip_I and days.date = '{day}'
                 and start_time_ds >= {st} and start_time_ds < {et}
                group by stop_times.trip_I) q1
                group by stop_I""".format(
        st=st, et=et, day=day
    )
    df = gtfs.execute_custom_query_pandas(query)
    df = gtfs.add_coordinates_to_df(df)
    return df


def get_vehicle_hours_by_type(gtfs, route_type):
    """
    Return the sum of vehicle hours in a particular day by route type.
    """

    day = gtfs.get_suitable_date_for_daily_extract()
    query = (
        " SELECT * , SUM(end_time_ds - start_time_ds)/3600 as vehicle_hours_type"
        " FROM"
        " (SELECT * FROM day_trips as q1"
        " INNER JOIN"
        " (SELECT route_I, type FROM routes) as q2"
        " ON q1.route_I = q2.route_I"
        " WHERE type = {route_type}"
        " AND date = '{day}')".format(day=day, route_type=route_type)
    )
    df = gtfs.execute_custom_query_pandas(query)
    return df["vehicle_hours_type"].item()


def trips_frequencies(gtfs):
    """
        Get the frequency of trip_I in a particular day
    """
    query = (
        " SELECT q1.stop_I as from_stop_I, q2.stop_I as to_stop_I, q1.trip_I as trip_I, COUNT(*) as freq FROM"
        " (SELECT * FROM stop_times) q1,"
        " (SELECT * FROM stop_times) q2"
        " WHERE q1.seq+1=q2.seq AND q1.trip_I=q2.trip_I"
        " GROUP BY from_stop_I, to_stop_I"
    )
    return gtfs.execute_custom_query_pandas(query)


# def route_circuity():
#    pass
