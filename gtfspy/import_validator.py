# reload(sys)
# -*- encoding: utf-8 -*-
import pandas as pd
from six import string_types
from gtfspy.timetable_validator import WarningsContainer

from gtfspy.gtfs import GTFS
from gtfspy.util import str_time_to_day_seconds
from gtfspy.util import txt_to_pandas

"""
input: sourcefile(s) of the gtfs data, created sqlite database
output: error messages for failed checks
- Assert that the sqlite database exists and that it is not corrupted
    -> it is possible to create a gtfs object
    -
- Row count of each table, check that each rowcount matches the imported files
    - Open each table in sqlite database and all text files in the zips
    - Compare
    - Should GTFS source file dirs be added to metadata so that the checkup can be made later?

- Assert that there are no Null values in essential data columns
    - which columns are essential?
    - Check that links between tables match (no unused rows) <- the file might be ok even if there is,
     but might be worth checking anyway. The loose ends should be removed in the filtering step.

WARNING_AGENCIES_NULL
WARNING_ROUTES_NULL
WARNING_TRIPS_NULL
WARNING_CALENDAR_NULL
WARNING_CALENDAR_DATES_NULL
WARNING_STOP_TIMES_NULL
WARNING_STOPS_NULL
WARNING_SHAPES_NULL
WARNING_AGENCIES_ROWS_MISSING
WARNING_ROUTES_ROWS_MISSING
WARNING_TRIPS_ROWS_MISSING
WARNING_CALENDAR_ROWS_MISSING
WARNING_CALENDAR_DATES_ROWS_MISSING
WARNING_STOP_TIMES_ROWS_MISSING
WARNING_STOPS_ROWS_MISSING
WARNING_SHAPES_ROWS_MISSING
"""

DANGLER_QUERIES = [
    'SELECT count(*) FROM stops LEFT JOIN stop_times ON(stop_times.stop_I=stops.stop_I) WHERE stop_times.stop_I IS NULL',
    'SELECT count(*) FROM stop_times LEFT JOIN stops ON(stop_times.stop_I=stops.stop_I) WHERE stops.stop_I IS NULL',
    'SELECT count(*) FROM stop_times LEFT JOIN trips ON(stop_times.trip_I=trips.trip_I) WHERE trips.trip_I IS NULL',
    'SELECT count(*) FROM trips LEFT JOIN stop_times ON(stop_times.trip_I=trips.trip_I) WHERE stop_times.trip_I IS NULL',
    'SELECT count(*) FROM trips LEFT JOIN days ON(days.trip_I=trips.trip_I) WHERE days.trip_I IS NULL',
    'SELECT count(*) FROM trips LEFT JOIN routes ON(routes.route_I=trips.route_I) WHERE routes.route_I IS NULL',
    'SELECT count(*) FROM days LEFT JOIN trips ON(days.trip_I=trips.trip_I) WHERE trips.trip_I IS NULL',
    'SELECT count(*) FROM routes LEFT JOIN trips ON(routes.route_I=trips.route_I) WHERE trips.route_I IS NULL'
]

WARNING_AGENCIES_NULL = "Null values found in agencies"
WARNING_ROUTES_NULL = "Null values found in routes"
WARNING_TRIPS_NULL = "Null values found in trips"
WARNING_CALENDAR_NULL = "Null values found in calendar"
WARNING_CALENDAR_DATES_NULL = "Null values found in calendar_dates"
WARNING_STOP_TIMES_NULL = "Null values found in stop_times"
WARNING_STOPS_NULL = "Null values found in stops"
WARNING_SHAPES_NULL = "Null values found in shapes"
WARNING_AGENCIES_ROWS_MISSING = "Rows missing in agencies"
WARNING_ROUTES_ROWS_MISSING = "Rows missing in routes"
WARNING_TRIPS_ROWS_MISSING = "Rows missing in trips"
WARNING_CALENDAR_ROWS_MISSING = "Rows missing in calendar"
WARNING_CALENDAR_DATES_ROWS_MISSING = "Rows missing in calendar_dates"
WARNING_STOP_TIMES_ROWS_MISSING = "Rows missing in stop_times"
WARNING_STOPS_ROWS_MISSING = "Rows missing in stops"
WARNING_SHAPES_ROWS_MISSING = "Rows missing in shapes"
WARNING_DANGLING_STOPS_VS_STOP_TIMES = "Stops not referenced in stop_time found"
WARNING_DANGLING_STOP_TIMES_VS_STOPS = "Stop_times referencing to missing stop"
WARNING_DANGLING_STOP_TIMES_VS_TRIPS = "Stop_times not referenced in trips found"
WARNING_DANGLING_TRIPS_VS_STOP_TIMES = "Trips with missing stop_times found"
WARNING_DANGLING_TRIPS_VS_DAYS = "Trips not referenced in days found"
WARNING_DANGLING_TRIPS_VS_ROUTES = "Trips not referenced in routes found"
WARNING_DANGLING_DAYS_VS_TRIPS = "Days not referenced in trips found"
WARNING_DANGLING_ROUTES_VS_TRIPS = "Routes not referenced in trips found"

NULL_WARNINGS = [
    WARNING_AGENCIES_NULL,
    WARNING_ROUTES_NULL,
    WARNING_TRIPS_NULL,
    WARNING_CALENDAR_NULL,
    WARNING_CALENDAR_DATES_NULL,
    WARNING_STOP_TIMES_NULL,
    WARNING_STOPS_NULL,
    WARNING_SHAPES_NULL
]

ROW_WARNINGS = [
    WARNING_AGENCIES_ROWS_MISSING,
    WARNING_ROUTES_ROWS_MISSING,
    WARNING_TRIPS_ROWS_MISSING,
    WARNING_CALENDAR_ROWS_MISSING,
    WARNING_CALENDAR_DATES_ROWS_MISSING,
    WARNING_STOP_TIMES_ROWS_MISSING,
    WARNING_STOPS_ROWS_MISSING,
    WARNING_SHAPES_ROWS_MISSING
]

DANGLER_WARNINGS = [
    WARNING_DANGLING_STOPS_VS_STOP_TIMES,
    WARNING_DANGLING_STOP_TIMES_VS_STOPS,
    WARNING_DANGLING_STOP_TIMES_VS_TRIPS,
    WARNING_DANGLING_TRIPS_VS_STOP_TIMES,
    WARNING_DANGLING_TRIPS_VS_DAYS,
    WARNING_DANGLING_TRIPS_VS_ROUTES,
    WARNING_DANGLING_DAYS_VS_TRIPS,
    WARNING_DANGLING_ROUTES_VS_TRIPS
]

FIELDS_WHERE_NULL_OK = {
                'agencies': ['lang', 'phone'],
                'routes': ['desc', 'url', 'color', 'text_color'],
                'trips': [],
                'calendar': [],
                'calendar_dates': [],
                'days': [],
                'shapes': [],
                'stop_times': [],
                'stops': ['code', 'desc', 'parent_I', 'wheelchair_boarding'],
                'stops_rtree': [],
                'stop_distances': []
            }

DB_TABLE_NAMES = ['agencies', 'routes', 'trips', 'calendar', 'calendar_dates', 'stop_times', 'stops', 'shapes']
SOURCE_TABLE_NAMES = ['agency', 'routes', 'trips', 'calendar', 'calendar_dates', 'stop_times', 'stops', 'shapes']

class ImportValidator(object):

    def __init__(self, gtfssource, gtfs):
        """
        Parameters
        ----------
        gtfs_sources: list of strings
        gtfs: GTFS, or path to a GTFS object
            A GTFS object
        """
        self.df_freq_dict = {}
        if isinstance(gtfssource, string_types + (dict,)):
            _gtfs_sources = [gtfssource]
        else:
            assert isinstance(gtfssource, list)
            _gtfs_sources = gtfssource

        self.gtfs_sources = _gtfs_sources

        if not isinstance(gtfs, GTFS):
            self.gtfs = GTFS(gtfs)
        else:
            self.gtfs = gtfs

        self.location = self.gtfs.get_location_name()
        self.warnings_container = WarningsContainer()

    def get_warnings(self):
        self.warnings_container.clear()
        self._validate_table_counts()
        self._validate_no_nulls()
        self._validate_danglers()
        self.warnings_container.print_summary()
        return self.warnings_container

    def _validate_table_counts(self):
        """
        Imports source .txt files, checks row counts and then compares the rowcounts with the gtfsobject
        :return:
        """
        row_count = {}
        for txt, table, row_warning in zip(SOURCE_TABLE_NAMES, DB_TABLE_NAMES, ROW_WARNINGS):
            row_count[txt] = 0

            for gtfs_source in self.gtfs_sources:
                frequencies_in_source = txt_to_pandas(gtfs_source, u'frequencies.txt')
                try:
                    if txt == 'trips' and not frequencies_in_source.empty:
                        row_count[txt] += self._frequency_generated_trips(gtfs_source, txt)

                    elif txt == 'stop_times' and not frequencies_in_source.empty:
                        row_count[txt] += self._frequency_generated_stop_times(gtfs_source, txt)

                    else:
                        df = txt_to_pandas(gtfs_source, txt)

                        row_count[txt] += len(df.index)
                except (IOError):
                    pass

            # Result from GTFSobj:
            table_counts = self.gtfs.get_row_count(table)
            print('row count for source ' +
                  str(txt) +
                  ' is ' +
                  str(row_count[txt]) +
                  ' while the corresponding rowcount in gtfsobject is ' +
                  str(table_counts))
            if not row_count[txt] == table_counts:
                # print('Warning: difference in row_count for table ' + table)
                self.warnings_container.add_warning(self.location, row_warning)

    def _validate_no_nulls(self):
        """
        Loads the tables from the gtfs object and counts the number of rows that have null values in
        fields that should not be null. Stores the number of null rows in warnings_container
        :return:
        """
        for table, null_warning in zip(DB_TABLE_NAMES, NULL_WARNINGS):
            # TODO: make this validation source by source
            df = self.gtfs.get_table(table)
            df.drop(FIELDS_WHERE_NULL_OK[table], inplace=True, axis=1)
            # print(df.to_string())
            len_table = len(df.index)
            df.dropna(inplace=True, axis=0)
            len_non_null = len(df.index)
            nullrows = len_table - len_non_null
            if nullrows > 0:
                # print('Warning: Null values detected in table ' + table)
                self.warnings_container.add_warning(self.location, null_warning, value=nullrows)

    def _validate_danglers(self):
        """
        Checks for rows that are not referenced in the the tables that should be linked

        stops <> stop_times using stop_I
        stop_times <> trips <> days, using trip_I
        trips <> routes, using route_I
        :return:
        """

        for query, warning in zip(DANGLER_QUERIES, DANGLER_WARNINGS):
            danglers = self.gtfs.execute_custom_query(query).fetchone()[0]
            print(str(danglers) + warning)
            if danglers > 0:
                self.warnings_container.add_warning(self.location, warning, value=danglers)

    def _frequency_generated_trips(self, source, txt):
        """
        This function calculates the equivalent rowcounts for trips when
        taking into account the generated rows in the gtfs object
        :param source: path to the source file
        :param txt: txt file in question
        :return: sum of all trips
        """
        df_freq = txt_to_pandas(source, u'frequencies.txt')
        df_trips = txt_to_pandas(source, txt)
        df_freq['n_trips'] = df_freq.apply(lambda row: len(range(str_time_to_day_seconds(row['start_time']),
                                                                 str_time_to_day_seconds(row['end_time']),
                                                                 row['headway_secs'])), axis=1)
        self.df_freq_dict[source] = df_freq
        df_trips_freq = pd.merge(df_freq, df_trips, how='outer', on='trip_id')

        return int(df_trips_freq['n_trips'].fillna(1).sum(axis=0))

    def _frequency_generated_stop_times(self, source, txt):
        """
        same as above except for stop times table
        :param source:
        :param txt:
        :return:
        """
        df_stop_times = txt_to_pandas(source, txt)
        df_freq = self.df_freq_dict[source]
        df_stop_freq = pd.merge(df_freq, df_stop_times, how='outer', on='trip_id')

        return int(df_stop_freq['n_trips'].fillna(1).sum(axis=0))

def main():
    pass

if __name__ == "__main__":
    main()
