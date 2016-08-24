from collections import defaultdict, Counter

from .util import wgs84_distance
from .gtfs import GTFS

WARNING_LONG_STOP_SPACING = "Long Stop Spacing"
WARNING_5_OR_MORE_CONSECUTIVE_STOPS_WITH_SAME_TIME = "5 Or More Consecutive Stop Times With Same Time"
WARNING_LONG_TRIP_TIME = "Long Trip Time"
WARNING_UNREALISTIC_AVERAGE_SPEED = "Unrealistic Average Speed"
WARNING_LONG_TRAVEL_TIME_BETWEEN_STOPS = "Long Travel Time Between Consecutive Stops"

ALL_WARNINGS = {
    WARNING_LONG_STOP_SPACING,
    WARNING_5_OR_MORE_CONSECUTIVE_STOPS_WITH_SAME_TIME,
    WARNING_LONG_TRIP_TIME,
    WARNING_UNREALISTIC_AVERAGE_SPEED,
    WARNING_LONG_TRAVEL_TIME_BETWEEN_STOPS
}

class GTFSValidator(object):

    def __init__(self, gtfs):
        """
        Parameters
        ----------
        gtfs: GTFS
            A GTFS object
        """
        self.warnings_container = ValidationWarningsContainer()
        self.conn = gtfs.conn
        self.cur = gtfs.get_cursor()

    def validate(self):
        """
        Validates/checks a given GTFS feed with respect to a number of different issues.

        The set of warnings that are checked for, can be found in the gtfs_validator.ALL_WARNINGS

        Returns
        -------
        warnings: ValidationWarningsContainer
        """
        self._validate_stops_with_same_stop_time()
        self._validate_speeds_and_trip_times()
        self._validate_stop_spacings()
        self.warnings_container.print_summary()
        return self.warnings_container

    def _validate_stops_with_same_stop_time(self):
        n_stops_with_same_time = 5
        # this query returns the trips where there are N or more stops with the same stop time
        rows = self.cur.execute(
            'select trip_I, arr_time, N from ( select trip_I, arr_time, count(*) as N '
            'from stop_times group by trip_I, arr_time) q1 where N >= ?', (n_stops_with_same_time,))
        for row in rows:
            self.warnings_container.add_warning(row, WARNING_5_OR_MORE_CONSECUTIVE_STOPS_WITH_SAME_TIME)

    def _validate_stop_spacings(self):
        self.conn.create_function("find_distance", 4, wgs84_distance)
        max_stop_spacing = 20000  # meters
        max_time_between_stops = 1800  # seconds
        # this query calculates distance and travel time between consecutive stops
        rows = self.cur.execute(
            'select q1.trip_I, type, q1.stop_I as stop_1, q2.stop_I as stop_2, '
            'CAST(find_distance(q1.lat, q1.lon, q2.lat, q2.lon) AS INT) as distance, '
            'q2.arr_time_ds - q1.arr_time_ds as traveltime '
            'from (select * from stop_times, '
            'stops where stop_times.stop_I = stops.stop_I) q1, (select * from stop_times, '
            'stops where stop_times.stop_I = stops.stop_I) q2, trips, routes where q1.trip_I = q2.trip_I '
            'and q1.seq + 1 = q2.seq and q1.trip_I = trips.trip_I and trips.route_I = routes.route_I ').fetchall()
        for row in rows:
            if row[4] > max_stop_spacing:
                self.warnings_container.add_warning(row, WARNING_LONG_STOP_SPACING)
            if row[5] > max_time_between_stops:
                self.warnings_container.add_warning(row, WARNING_LONG_TRAVEL_TIME_BETWEEN_STOPS)

    def _validate_speeds_and_trip_times(self):
        # These are the mode - feasible speed combinations used here:
        # https://support.google.com/transitpartners/answer/1095482?hl=en
        type_speed = {
            0: ("Tram", 100),
            1: ("Subway", 150),
            2: ("Rail", 300),
            3: ("Bus", 100),
            4: ("Ferry", 80),
            5: ("Cable Car", 50),
            6: ("Gondola", 50),
            7: ("Funicular", 50)
        }
        max_trip_time = 7200  # seconds
        self.conn.create_function("find_distance", 4, wgs84_distance)
        rows = self.cur.execute(
            'SELECT '
            ' q1.trip_I, '
            ' type, '
            ' sum(CAST(find_distance(q1.lat, q1.lon, q2.lat, q2.lon) AS INT)) AS total_distance, '
            ' sum(q2.arr_time_ds - q1.arr_time_ds) AS total_traveltime '
            ' FROM '
            '(SELECT * FROM stop_times, '
            'stops WHERE stop_times.stop_I = stops.stop_I) q1, (SELECT * FROM stop_times, '
            'stops WHERE stop_times.stop_I = stops.stop_I) q2, trips, routes WHERE q1.trip_I = q2.trip_I '
            'AND q1.seq + 1 = q2.seq AND q1.trip_I = trips.trip_I '
            '  AND trips.route_I = routes.route_I GROUP BY q1.trip_I').fetchall()

        for row in rows:
            avg_velocity = row[2] / row[3] * 3.6
            if avg_velocity > type_speed[row[1]][1]:
                self.warnings_container.add_warning(row, WARNING_UNREALISTIC_AVERAGE_SPEED)

            if row[3] > max_trip_time:
                self.warnings_container.add_warning(row, WARNING_LONG_TRIP_TIME)


class ValidationWarningsContainer(object):

    def __init__(self):
        self._warnings_counter = Counter()
        # key: "warning type" string, value: "number of errors" int
        self._warnings_records = defaultdict(list)
        # key: "row that produced error" tuple, value: list of "warning type(s)" string

    def add_warning(self, row, error):
        self._warnings_counter[error] += 1
        self._warnings_records[row].append(error)

    def print_summary(self):
        for key in self._warnings_counter.keys():
            print key + ": " + str(self._warnings_counter[key])

    def get_warning_counts(self):
        """
        Returns
        -------
        counter: collections.Counter
        """
        return self._warnings_counter

    def get_warnings_by_query_rows(self):
        """
        Returns
        -------
        warnings_record: defaultdict(list)
            maps each row to a list of warnings
        """
        return self._warnings_records
