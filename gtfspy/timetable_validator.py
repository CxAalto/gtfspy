import sys
import pandas

# the following is required when using this module as a script
# (i.e. using the if __name__ == "__main__": part at the end of this file)
from gtfspy.warnings_container import WarningsContainer

if __name__ == '__main__' and __package__ is None:
    # import gtfspy
    __package__ = 'gtfspy'


from gtfspy import route_types
from gtfspy.gtfs import GTFS
from gtfspy.util import wgs84_distance


WARNING_5_OR_MORE_CONSECUTIVE_STOPS_WITH_SAME_TIME = "trip--arr_time -combinations with five or more consecutive stops having same stop time"
WARNING_LONG_TRIP_TIME = "Trip time longer than {MAX_TRIP_TIME} seconds"
WARNING_TRIP_UNREALISTIC_AVERAGE_SPEED = "trips whose average speed is unrealistic relative to travel mode"

MAX_ALLOWED_DISTANCE_BETWEEN_CONSECUTIVE_STOPS = 20000  # meters
WARNING_LONG_STOP_SPACING = "distance between consecutive stops longer than " + str(MAX_ALLOWED_DISTANCE_BETWEEN_CONSECUTIVE_STOPS) + " meters"
MAX_TIME_BETWEEN_STOPS = 1800  # seconds
WARNING_LONG_TRAVEL_TIME_BETWEEN_STOPS = "trip--stop_times-combinations with travel time between consecutive stops longer than " + str(MAX_TIME_BETWEEN_STOPS / 60) + " minutes"

WARNING_STOP_SEQUENCE_ORDER_ERROR = "stop sequence is not in right order"
WARNING_STOP_SEQUENCE_NOT_INCREMENTAL = "stop sequences are not increasing always by one in stop_times"
WARNING_STOP_FAR_AWAY_FROM_FILTER_BOUNDARY = "stop far away from spatial filter boundary"

ALL_WARNINGS = {
    WARNING_LONG_STOP_SPACING,
    WARNING_5_OR_MORE_CONSECUTIVE_STOPS_WITH_SAME_TIME,
    WARNING_LONG_TRIP_TIME,
    WARNING_TRIP_UNREALISTIC_AVERAGE_SPEED,
    WARNING_LONG_TRAVEL_TIME_BETWEEN_STOPS,
    WARNING_STOP_SEQUENCE_ORDER_ERROR,
    WARNING_STOP_SEQUENCE_NOT_INCREMENTAL
}

GTFS_TYPE_TO_MAX_SPEED = {
    route_types.TRAM: 100,
    route_types.SUBWAY: 150,
    route_types.RAIL: 300,
    route_types.BUS: 100,
    route_types.FERRY: 80,
    route_types.CABLE_CAR: 50,
    route_types.GONDOLA: 50,
    route_types.FUNICULAR: 50,
    route_types.AIRCRAFT: 1000
}
MAX_TRIP_TIME = 7200  # seconds

class TimetableValidator(object):

    def __init__(self, gtfs, buffer_params=None):
        """
        Parameters
        ----------
        gtfs: GTFS, or path to a GTFS object
            A GTFS object
        """
        if not isinstance(gtfs, GTFS):
            self.gtfs = GTFS(gtfs)
        else:
            self.gtfs = gtfs
        self.buffer_params = buffer_params
        self.warnings_container = WarningsContainer()

    def validate_and_get_warnings(self):
        """
        Validates/checks a given GTFS feed with respect to a number of different issues.

        The set of warnings that are checked for, can be found in the gtfs_validator.ALL_WARNINGS

        Returns
        -------
        warnings: WarningsContainer
        """
        self.warnings_container.clear()
        self._validate_stops_with_same_stop_time()
        self._validate_speeds_and_trip_times()
        self._validate_stop_spacings()
        self._validate_stop_sequence()
        self._validate_misplaced_stops()
        return self.warnings_container

    def _validate_misplaced_stops(self):
        if self.buffer_params:
            p = self.buffer_params
            center_lat = p['lat']
            center_lon = p['lon']
            buffer_distance = p['buffer_distance'] * 100 * 1.002 # some error margin for rounding
            for stop_row in self.gtfs.stops().itertuples():
                if buffer_distance < wgs84_distance(center_lat, center_lon, stop_row.lat, stop_row.lon):
                    self.warnings_container.add_warning(WARNING_STOP_FAR_AWAY_FROM_FILTER_BOUNDARY, stop_row)
                    print(WARNING_STOP_FAR_AWAY_FROM_FILTER_BOUNDARY, stop_row)

    def _validate_stops_with_same_stop_time(self):
        n_stops_with_same_time = 5
        # this query returns the trips where there are N or more stops with the same stop time
        rows = self.gtfs.get_cursor().execute(
            'SELECT '
            'trip_I, '
            'arr_time, '
            'N '
            'FROM '
            '(SELECT trip_I, arr_time, count(*) as N FROM stop_times GROUP BY trip_I, arr_time) q1 '
            'WHERE N >= ?', (n_stops_with_same_time,)
        )
        for row in rows:
            self.warnings_container.add_warning(WARNING_5_OR_MORE_CONSECUTIVE_STOPS_WITH_SAME_TIME, row)

    def _validate_stop_spacings(self):
        self.gtfs.conn.create_function("find_distance", 4, wgs84_distance)
        # this query calculates distance and travel time between consecutive stops
        rows = self.gtfs.execute_custom_query(
            'SELECT '
            'q1.trip_I, '
            'type, '
            'q1.stop_I as stop_1, '
            'q2.stop_I as stop_2, '
            'CAST(find_distance(q1.lat, q1.lon, q2.lat, q2.lon) AS INT) as distance, '
            'q2.arr_time_ds - q1.arr_time_ds as traveltime '
            'FROM '
            '(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) q1, '
            '(SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) q2, '
            'trips, '
            'routes '
            'WHERE q1.trip_I = q2.trip_I '
            'AND q1.seq + 1 = q2.seq '
            'AND q1.trip_I = trips.trip_I '
            'AND trips.route_I = routes.route_I ').fetchall()
        for row in rows:
            if row[4] > MAX_ALLOWED_DISTANCE_BETWEEN_CONSECUTIVE_STOPS:
                self.warnings_container.add_warning(WARNING_LONG_STOP_SPACING, row)
            if row[5] > MAX_TIME_BETWEEN_STOPS:
                self.warnings_container.add_warning(WARNING_LONG_TRAVEL_TIME_BETWEEN_STOPS, row)

    def _validate_speeds_and_trip_times(self):
        # These are the mode - feasible speed combinations used here:
        # https://support.google.com/transitpartners/answer/1095482?hl=en
        self.gtfs.conn.create_function("find_distance", 4, wgs84_distance)

        # this query returns the total distance and travel time for each trip calculated for each stop spacing separately
        rows = pandas.read_sql(
            'SELECT '
            'q1.trip_I, '
            'type, '
            'sum(CAST(find_distance(q1.lat, q1.lon, q2.lat, q2.lon) AS INT)) AS total_distance, ' # sum used for getting total
            'sum(q2.arr_time_ds - q1.arr_time_ds) AS total_traveltime, ' # sum used for getting total
            'count(*)' # for getting the total number of stops
            'FROM '
            '   (SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) q1, '
            '   (SELECT * FROM stop_times, stops WHERE stop_times.stop_I = stops.stop_I) q2, '
            '    trips, '
            '    routes '
                'WHERE q1.trip_I = q2.trip_I AND q1.seq + 1 = q2.seq AND q1.trip_I = trips.trip_I '
                     'AND trips.route_I = routes.route_I GROUP BY q1.trip_I', self.gtfs.conn)

        for row in rows.itertuples():
            avg_velocity_km_per_h = row.total_distance / max(row.total_traveltime, 1) * 3.6
            if avg_velocity_km_per_h > GTFS_TYPE_TO_MAX_SPEED[row.type]:
                self.warnings_container.add_warning(WARNING_TRIP_UNREALISTIC_AVERAGE_SPEED + " (route_type=" + str(row.type) + ")",
                                                    row
                )
            if row.total_traveltime > MAX_TRIP_TIME:
                self.warnings_container.add_warning(WARNING_LONG_TRIP_TIME.format(MAX_TRIP_TIME=MAX_TRIP_TIME), row, 1)

    def _validate_stop_sequence(self):
        # This function checks if the seq values in stop_times are increasing with departure_time,
        # and that seq always increases by one.
        rows = self.gtfs.execute_custom_query('SELECT trip_I, dep_time_ds, seq '
                                              'FROM stop_times '
                                              'ORDER BY trip_I, dep_time_ds, seq').fetchall()
        old_trip_id = None
        old_seq = None
        for row in rows:
            new_trip_id = int(row[0])
            new_seq = int(row[2])
            if old_trip_id == new_trip_id:
                if old_seq + 1 != new_seq:
                    self.warnings_container.add_warning(WARNING_STOP_SEQUENCE_NOT_INCREMENTAL, row)
                if old_seq >= new_seq:
                    self.warnings_container.add_warning(WARNING_STOP_SEQUENCE_ORDER_ERROR, row)
            old_trip_id = row[0]
            old_seq = row[2]


def main():
    cmd = sys.argv[1]
    args = sys.argv[2:]
    if cmd == "validate":
        validator = TimetableValidator(args[0])
        warningscontainer = validator.validate_and_get_warnings()
        warningscontainer.write_summary()

if __name__ == "__main__":
    main()

