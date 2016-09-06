import csv
import pandas as pd

import numpy

from gtfspy.gtfs import GTFS
from gtfspy.util import wgs84_distance


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
    median_lat = numpy.percentile(stops['lat'].values, 50)
    median_lon = numpy.percentile(stops['lon'].values, 50)
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
    mean_lat = numpy.mean(stops['lat'].values)
    mean_lon = numpy.mean(stops['lon'].values)
    return mean_lat, mean_lon


def write_stats_as_csv(gtfs, path_to_csv):
    """
    Writes data from get_stats to csv file

    Parameters
    ----------
    gtfs: GTFS
    path_to_csv: str
        filepath to the csv file to be generated
    """
    stats_dict = get_stats(gtfs)
    # check if file exist
    """if not os.path.isfile(path_to_csv):
        is_new = True
    else:
        is_new = False"""
    try:
        with open(path_to_csv, 'rb') as csvfile:
            if list(csv.reader(csvfile))[0]:
                is_new = False
            else:
                is_new = True
    except Exception as e:
        is_new = True

    with open(path_to_csv, 'ab') as csvfile:
        statswriter = csv.writer(csvfile, delimiter=',')
        # write column names if new file
        if is_new:
            statswriter.writerow(sorted(stats_dict.keys()))
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
    for table in ['agencies', 'routes', 'stops', 'stop_times', 'trips', 'calendar', 'shapes', 'calendar_dates',
                  'days', 'stop_distances', 'frequencies', 'feed_info', 'transfers']:
        stats["n_" + table] = gtfs.get_row_count(table)

    # Agency names
    agencies = gtfs.get_table("agencies")
    stats["agencies"] = "_".join(agencies['name'].values).encode(
        'utf-8')

    # Stop lat/lon range
    stops = gtfs.get_table("stops")
    lats = stops['lat'].values
    lons = stops['lon'].values
    percentiles = [0, 10, 50, 90, 100]

    lat_min, lat_10, lat_median, lat_90, lat_max = numpy.percentile(lats, percentiles)
    stats["lat_min"] = lat_min
    stats["lat_10"] = lat_10
    stats["lat_median"] = lat_median
    stats["lat_90"] = lat_90
    stats["lat_max"] = lat_max

    lon_min, lon_10, lon_median, lon_90, lon_max = numpy.percentile(lons, percentiles)
    stats["lon_min"] = lon_min
    stats["lon_10"] = lon_10
    stats["lon_median"] = lon_median
    stats["lon_90"] = lon_90
    stats["lon_max"] = lon_max

    stats["height_km"] = wgs84_distance(lat_min, lon_median, lat_max, lon_median) / 1000.
    stats["width_km"] = wgs84_distance(lon_min, lat_median, lon_max, lat_median) / 1000.

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
        'SELECT count(*), date '
        'FROM days '
        'GROUP BY date '
        'ORDER BY count(*) DESC, date '
        'LIMIT 1;').fetchone()
    if max_activity_date:
        stats["max_activity_date"] = max_activity_date[1]
        max_activity_hour = gtfs.get_cursor().execute(
            'SELECT count(*), arr_time_hour FROM day_stop_times '
            'WHERE date=? GROUP BY arr_time_hour '
            'ORDER BY count(*) DESC;', (stats["max_activity_date"],)).fetchone()
        if max_activity_hour:
            stats["max_activity_hour"] = max_activity_hour[1]
        else:
            stats["max_activity_hour"] = None
    # Fleet size estimate: considering each line separately
    fleet_size_estimates = _fleet_size_estimate(gtfs, stats['max_activity_hour'], stats['max_activity_date'])
    stats.update(fleet_size_estimates)

    # Compute simple distributions of various columns that have a finite range of values.
    # Commented lines refer to values that are not imported yet, ?

    stats['routes__type__dist'] = _distribution(gtfs, 'routes', 'type')
    # stats['stop_times__pickup_type__dist'] = _distribution(gtfs, 'stop_times', 'pickup_type')
    # stats['stop_times__drop_off_type__dist'] = _distribution(gtfs, 'stop_times', 'drop_off_type')
    # stats['stop_times__timepoint__dist'] = _distribution(gtfs, 'stop_times', 'timepoint')
    stats['calendar_dates__exception_type__dist'] = _distribution(gtfs, 'calendar_dates', 'exception_type')
    stats['frequencies__exact_times__dist'] = _distribution(gtfs, 'frequencies', 'exact_times')
    stats['transfers__transfer_type__dist'] = _distribution(gtfs, 'transfers', 'transfer_type')
    stats['agencies__lang__dist'] = _distribution(gtfs, 'agencies', 'lang')
    stats['stops__location_type__dist'] = _distribution(gtfs, 'stops', 'location_type')
    # stats['stops__wheelchair_boarding__dist'] = _distribution(gtfs, 'stops', 'wheelchair_boarding')
    # stats['trips__wheelchair_accessible__dist'] = _distribution(gtfs, 'trips', 'wheelchair_accessible')
    # stats['trips__bikes_allowed__dist'] = _distribution(gtfs, 'trips', 'bikes_allowed')
    # stats[''] = _distribution(gtfs, '', '')
    return stats


def _distribution(gtfs, table, column):
    """Count occurrences of values AND return it as a string.

    Example return value:   '1:5 2:15'"""
    cur = gtfs.conn.cursor()
    cur.execute('SELECT {column}, count(*) '
                'FROM {table} GROUP BY {column} '
                'ORDER BY {column}'.format(column=column, table=table))
    return ' '.join('%s:%s' % (t, c) for t, c in cur)


def _fleet_size_estimate(gtfs, hour, date):
    """
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
        'SELECT type, max(vehicles) '
            'FROM ('
                'SELECT type, direction_id, sum(vehicles) as vehicles '
                'FROM '
                '('
                    'SELECT trips.route_I, trips.direction_id, routes.route_id, name, type, count(*) as vehicles, cycle_time_min '
                    'FROM trips, routes, days, '
                    '('
                        'SELECT first_trip.route_I, first_trip.direction_id, first_trip_start_time, first_trip_end_time, '
                            'MIN(start_time_ds) as return_trip_start_time, end_time_ds as return_trip_end_time, '
                            '(end_time_ds - first_trip_start_time)/60 as cycle_time_min '
                        'FROM '
                            'trips, '
                            '(SELECT route_I, direction_id, MIN(start_time_ds) as first_trip_start_time, '
                                    'end_time_ds as first_trip_end_time '
                             'FROM trips, days '
                             'WHERE trips.trip_I=days.trip_I AND start_time_ds >= ? * 3600 '
                                'AND start_time_ds <= (? + 1) * 3600 AND date = ? '
                             'GROUP BY route_I, direction_id) first_trip '
                        'WHERE first_trip.route_I = trips.route_I '
                            'AND first_trip.direction_id != trips.direction_id '
                            'AND start_time_ds >= first_trip_end_time '
                        'GROUP BY trips.route_I, trips.direction_id'
                    ') return_trip '
                    'WHERE trips.trip_I=days.trip_I AND trips.route_I= routes.route_I '
                        'AND date = ? AND trips.route_I = return_trip.route_I '
                        'AND trips.direction_id = return_trip.direction_id '
                        'AND start_time_ds >= first_trip_start_time '
                        'AND start_time_ds < return_trip_end_time '
                    'GROUP BY trips.route_I, trips.direction_id '
                    'ORDER BY type, name, vehicles desc'
                ') cycle_times '
                'GROUP BY direction_id, type'
                ') vehicles_type '
            'GROUP BY type;', (hour, hour, date, date))
    for row in rows:
        fleet_size_list.append(str(row[0]) + ':' + str(row[1]))
    results['fleet_size_route_based'] = " ".join(fleet_size_list)

    # Fleet size estimate: maximum number of vehicles in movement
    fleet_size_list = []
    fleet_size_dict = {}
    if hour:
        for minute in range(hour * 3600, (hour + 1) * 3600, 60):
            rows = gtfs.conn.cursor().execute(
                'SELECT type, count(*) '
                'FROM trips, routes, days '
                'WHERE trips.route_I = routes.route_I '
                'AND trips.trip_I=days.trip_I '
                'AND start_time_ds <= ? '
                'AND end_time_ds > ? + 60 '
                'AND date = ? '
                'GROUP BY type;',
                (minute, minute, date))

            for row in rows:
                if fleet_size_dict.get(row[0], 0) < row[1]:
                    fleet_size_dict[row[0]] = row[1]

    for key in fleet_size_dict.keys():
        fleet_size_list.append(str(key) + ':' + str(fleet_size_dict[key]))
    results["fleet_size_max_movement"] = ' '.join(fleet_size_list)
    return results

def update_stats(gtfs):
    """
    Computes stats AND stores them into the underlying gtfs object (i.e. database).

    Parameters
    ----------
    gtfs: GTFS
    """
    stats = get_stats(gtfs)
    gtfs.update_stats(stats)

def route_distributions(gtfs):
    conn = gtfs.conn

    conn.create_function("find_distance", 4, wgs84_distance)
    cur = conn.cursor()
    # this query calculates the distance and travel time for each complete trip
    # stop_data_df = pd.read_sql_query(query, self.conn, params=params)

    query = 'SELECT ' \
            'startstop.trip_I AS trip_I, ' \
            'type, ' \
            'sum(CAST(find_distance(startstop.lat, startstop.lon, endstop.lat, endstop.lon) AS INT)) as total_distance, ' \
            'sum(endstop.arr_time_ds - startstop.arr_time_ds) as total_traveltime ' \
            'FROM ' \
            '(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) startstop, ' \
            '(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) endstop, ' \
            'trips, ' \
            'routes ' \
            'WHERE ' \
            'startstop.trip_I = endstop.trip_I ' \
            'AND startstop.seq + 1 = endstop.seq ' \
            'AND startstop.trip_I = trips.trip_I ' \
            'AND trips.route_I = routes.route_I ' \
            'GROUP BY startstop.trip_I'

    q_result = pd.read_sql_query(query, conn)
    q_result['avg_speed_kmh'] = 3.6 * q_result['total_distance'] / q_result['total_traveltime']
    q_result['total_distance'] = q_result['total_distance'] / 1000
    q_result['total_traveltime'] = q_result['total_traveltime'] / 60
    q_result = q_result.loc[q_result['avg_speed_kmh'] == float("inf")]
    return q_result
