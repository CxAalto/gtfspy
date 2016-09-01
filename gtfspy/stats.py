from __future__ import absolute_import

import numpy
from .gtfs import GTFS

from .util import wgs84_distance


class Stats(object):

    def __init__(self, gtfs):
        """
        Parameters
        ----------
        gtfs: GTFS, or path to a GTFS object
            A GTFS object
        """
        if isinstance(gtfs, GTFS):
            self.gtfs = gtfs
        else:
            self.gtfs = GTFS(gtfs)

    def get_median_lat_lon_of_stops(self):
        """
        Get median latitude and longitude of stops

        Returns
        -------
        median_lat : float
        median_lon : float
        """
        stops = self.gtfs.get_table("stops")
        median_lat = numpy.percentile(stops['lat'].values, 50)
        median_lon = numpy.percentile(stops['lon'].values, 50)
        return median_lat, median_lon

    def get_centroid_of_stops(self):
        """
        Get mean latitude and longitude of stops

        Returns
        -------
        mean_lat : float
        mean_lon : float
        """
        stops = self.gtfs.get_table("stops")
        mean_lat = numpy.mean(stops['lat'].values)
        mean_lon = numpy.mean(stops['lon'].values)
        return mean_lat, mean_lon

    def write_stats_as_csv(self, path_to_csv):
        """
        Writes data from get_stats to csv file

        Parameters
        ----------
        path_to_csv: filepath to csv file
        """
        import csv
        stats_dict = self.get_stats()
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
        except:
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

    def get_stats(self):
        """
        Get basic statistics of the GTFS data.

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
            stats["n_" + table] = self.gtfs.get_row_count(table)

        # Agency names
        agencies = self.gtfs.get_table("agencies")
        stats["agencies"] = "_".join(agencies['name'].values).encode(
            'utf-8')

        # Stop lat/lon range
        stops = self.gtfs.get_table("stops")
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

        first_day_start_ut, last_day_start_ut = self.gtfs.get_day_start_ut_span()
        stats["start_time_ut"] = first_day_start_ut
        if last_day_start_ut is None:
            stats["end_time_ut"] = None
        else:
            # 28 (instead of 24) comes from the GTFS standard
            stats["end_time_ut"] = last_day_start_ut + 28 * 3600

        stats["start_date"] = self.gtfs.get_min_date()
        stats["end_date"] = self.gtfs.get_max_date()

        # Maximum activity day
        max_activity_date = self.gtfs.execute_custom_query(
            'SELECT count(*), date FROM days GROUP BY date '
            'ORDER BY count(*) DESC, date LIMIT 1;').fetchone()
        if max_activity_date:
            stats["max_activity_date"] = max_activity_date[1]
            max_activity_hour = self.gtfs.get_cursor().execute(
                'SELECT count(*), arr_time_hour FROM day_stop_times '
                'WHERE date=? GROUP BY arr_time_hour '
                'ORDER BY count(*) DESC;', (stats["max_activity_date"],)).fetchone()
            if max_activity_hour:
                stats["max_activity_hour"] = max_activity_hour[1]
            else:
                stats["max_activity_hour"] = None
        # Fleet size estimate: considering each line separately
        fleet_size_list = []
        for row in self.gtfs.conn.cursor().execute(
                               'Select type, max(vehicles) from '
                               '(select type, direction_id, sum(vehicles) as vehicles from '
                               '(select trips.route_I, trips.direction_id, routes.route_id, name, type, count(*) as vehicles, cycle_time_min from trips, routes, days, '
                               '(select first_trip.route_I, first_trip.direction_id, first_trip_start_time, first_trip_end_time, '
                               'MIN(start_time_ds) as return_trip_start_time, end_time_ds as return_trip_end_time, '
                               '(end_time_ds - first_trip_start_time)/60 as cycle_time_min from trips, '
                               '(select route_I, direction_id, MIN(start_time_ds) as first_trip_start_time, end_time_ds as first_trip_end_time from trips, days '
                               'where trips.trip_I=days.trip_I and start_time_ds >= ? * 3600 and start_time_ds <= (? + 1) * 3600 and date = ? '
                               'group by route_I, direction_id) first_trip '
                               'where first_trip.route_I = trips.route_I and first_trip.direction_id != trips.direction_id and start_time_ds >= first_trip_end_time '
                               'group by trips.route_I, trips.direction_id) return_trip '
                               'where trips.trip_I=days.trip_I and trips.route_I= routes.route_I and date = ? and trips.route_I = return_trip.route_I and trips.direction_id = return_trip.direction_id and start_time_ds >= first_trip_start_time and start_time_ds < return_trip_end_time '
                               'group by trips.route_I, trips.direction_id '
                               'order by type, name, vehicles desc) cycle_times '
                               'group by direction_id, type) vehicles_type '
                               'group by type;', (
                               stats["max_activity_hour"], stats["max_activity_hour"],
                               stats["max_activity_date"], stats["max_activity_date"])):
            fleet_size_list.append(str(row[0]) + ':' + str(row[1]))
        stats["fleet_size_route_based"] = ' '.join(fleet_size_list)
        # Fleet size estimate: maximum number of vehicles in movement
        fleet_size_dict = {}
        fleet_size_list = []
        if stats["max_activity_hour"]:
            for minute in range(stats["max_activity_hour"] * 3600, (stats["max_activity_hour"] + 1) * 3600, 60):
                for row in self.gtfs.conn.cursor().execute('SELECT type, count(*) FROM trips, routes, days '
                                       'WHERE trips.route_I = routes.route_I AND trips.trip_I=days.trip_I AND '
                                       'start_time_ds <= ? AND end_time_ds > ? + 60 AND date = ? '
                                       'GROUP BY type;', (minute, minute, stats["max_activity_date"])):

                    if fleet_size_dict.get(row[0], 0) < row[1]:
                        fleet_size_dict[row[0]] = row[1]

        for key in fleet_size_dict.keys():
            fleet_size_list.append(str(key) + ':' + str(fleet_size_dict[key]))
        stats["fleet_size_max_movement"] = ' '.join(fleet_size_list)

        # Compute simple distributions of various colums that have a
        # finite range of values.
        def distribution(table, column):
            """Count occurances of values and return it as a string.

            Example return value:   '1:5 2:15'"""
            cur = self.gtfs.conn.cursor()
            cur.execute('SELECT {column}, count(*) '
                        'FROM {table} GROUP BY {column} '
                        'ORDER BY {column}'.format(column=column, table=table))
            return ' '.join('%s:%s' % (t, c) for t, c in cur)
            # Commented lines refer to values that are not imported yet.

        stats['routes__type__dist'] = distribution('routes', 'type')
        # stats['stop_times__pickup_type__dist'] = distribution('stop_times', 'pickup_type')
        # stats['stop_times__drop_off_type__dist'] = distribution('stop_times', 'drop_off_type')
        # stats['stop_times__timepoint__dist'] = distribution('stop_times', 'timepoint')
        stats['calendar_dates__exception_type__dist'] = distribution('calendar_dates', 'exception_type')
        stats['frequencies__exact_times__dist'] = distribution('frequencies', 'exact_times')
        stats['transfers__transfer_type__dist'] = distribution('transfers', 'transfer_type')
        stats['agencies__lang__dist'] = distribution('agencies', 'lang')
        stats['stops__location_type__dist'] = distribution('stops', 'location_type')
        # stats['stops__wheelchair_boarding__dist'] = distribution('stops', 'wheelchair_boarding')
        # stats['trips__wheelchair_accessible__dist'] = distribution('trips', 'wheelchair_accessible')
        # stats['trips__bikes_allowed__dist'] = distribution('trips', 'bikes_allowed')
        # stats[''] = distribution('', '')
        return stats

    def update_stats(self):
        """
        Computes stats and stores them into the underlying gtfs object (i.e. database).
        """
        stats = self.get_stats()
        self.gtfs.update_stats(stats)
