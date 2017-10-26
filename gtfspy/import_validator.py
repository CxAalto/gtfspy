import pandas as pd
import sys
from six import string_types
from gtfspy.timetable_validator import WarningsContainer
from gtfspy.gtfs import GTFS
from gtfspy.util import str_time_to_day_seconds
from gtfspy.util import source_csv_to_pandas

"""
Input: sourcefile(s) of the gtfs data, created sqlite database
output: error messages for failed checks
- Assert that the sqlite database exists and that it is not corrupted
    -> it is possible to create a gtfs object
    -
- Row count of each table, check that each rowcount matches the imported files
    - Open each table in sqlite database and all text files in the zips
    - Compare
    - Should GTFS source file dirs be added to metadata so that the checkup can be made later?

"""


WARNING_DANGLING_STOPS_VS_STOP_TIMES_AND_PARENT_STOPS = "Some stops not referenced in stop_times nor marked as a parent_stop were found (it is ok/possible that there are some)"
WARNING_DANGLING_STOP_TIMES_VS_STOPS = "stop_times referencing to missing stop"
WARNING_DANGLING_STOP_TIMES_VS_TRIPS = "stop_times not referenced in trips found"
WARNING_DANGLING_TRIPS_VS_STOP_TIMES = "trips with missing stop_times found"
WARNING_DANGLING_TRIPS_VS_DAYS = "trips not referenced in days found (this is possible due to some combinations of calendar and calendar_dates)"
WARNING_DANGLING_TRIPS_VS_CALENDAR = "trips whose service_I not referenced in calendar found"
WARNING_DANGLING_TRIPS_VS_ROUTES = "trips not referenced in routes found"
WARNING_DANGLING_DAYS_VS_TRIPS = "days not referenced in trips found"
WARNING_DANGLING_ROUTES_VS_TRIPS = "routes not referenced in trips found"


DANGLER_QUERIES = [
    'SELECT count(*) FROM stops '
        'LEFT JOIN stop_times ON(stop_times.stop_I=stops.stop_I) '
        'LEFT JOIN stops as parents ON(stops.stop_I=parents.parent_I) '
        'WHERE (stop_times.stop_I IS NULL AND parents.parent_I IS NULL)',
    'SELECT count(*) FROM stop_times LEFT JOIN stops ON(stop_times.stop_I=stops.stop_I) WHERE stops.stop_I IS NULL',
    'SELECT count(*) FROM stop_times LEFT JOIN trips ON(stop_times.trip_I=trips.trip_I) WHERE trips.trip_I IS NULL',
    'SELECT count(*) FROM trips LEFT JOIN stop_times ON(stop_times.trip_I=trips.trip_I) WHERE stop_times.trip_I IS NULL',
    'SELECT count(*) FROM trips LEFT JOIN days ON(days.trip_I=trips.trip_I) WHERE days.trip_I IS NULL',
    'SELECT count(*) FROM trips LEFT JOIN calendar ON(calendar.service_I=trips.service_I) WHERE trips.service_I IS NULL',
    'SELECT count(*) FROM trips LEFT JOIN routes ON(routes.route_I=trips.route_I) WHERE routes.route_I IS NULL',
    'SELECT count(*) FROM days LEFT JOIN trips ON(days.trip_I=trips.trip_I) WHERE trips.trip_I IS NULL',
    'SELECT count(*) FROM routes LEFT JOIN trips ON(routes.route_I=trips.route_I) WHERE trips.route_I IS NULL'
]

DANGLER_WARNINGS = [
    WARNING_DANGLING_STOPS_VS_STOP_TIMES_AND_PARENT_STOPS,
    WARNING_DANGLING_STOP_TIMES_VS_STOPS,
    WARNING_DANGLING_STOP_TIMES_VS_TRIPS,
    WARNING_DANGLING_TRIPS_VS_STOP_TIMES,
    WARNING_DANGLING_TRIPS_VS_DAYS,
    WARNING_DANGLING_TRIPS_VS_CALENDAR,
    WARNING_DANGLING_TRIPS_VS_ROUTES,
    WARNING_DANGLING_DAYS_VS_TRIPS,
    WARNING_DANGLING_ROUTES_VS_TRIPS,
]

DB_TABLE_NAME_TO_FIELDS_WHERE_NULL_NOT_OK = {
    'agencies': ['agency_I', 'agency_id', "timezone"],
    'stops': ['stop_I', 'stop_id', 'lat', 'lon'],
    'routes': ['route_I', 'route_id', 'type'],
    'trips': ['trip_I', 'trip_id', 'service_I', "route_I"],
    'stop_times': ["trip_I", "stop_I", "arr_time_ds", "dep_time_ds"],
    'calendar': ['service_id', 'service_I', 'm', "t", "w", "th", "f", "s", "su", "start_date", "end_date"],
    'calendar_dates': ['service_I', 'date', 'exception_type'],
    'days': ["date","day_start_ut","trip_I"],
    'shapes': ["shape_id", "lat", "lon", "seq"],
    'stop_distances': ["from_stop_I", "to_stop_I", "d", "d_walk"]
}

DB_TABLE_NAME_TO_FIELDS_WHERE_NULL_OK_BUT_WARN = {
    'agencies': ['name', "url"],
    'stops': ['name'],
    'routes': ['name', 'long_name'],
    'trips': [],
    'calendar': [],
    'calendar_dates': [],
    'days': [],
    'shapes': [],
    'stop_times': [],
    'stop_distances': []
}

DB_TABLE_NAMES = list(sorted(DB_TABLE_NAME_TO_FIELDS_WHERE_NULL_NOT_OK.keys()))

DB_TABLE_NAME_TO_SOURCE_FILE = {
    'agencies': "agency",
    'routes': "routes",
    'trips': "trips",
    'calendar': "calendar",
    'calendar_dates': "calendar_dates",
    'stop_times': "stop_times",
    'stops': "stops",
    "shapes": 'shapes'
}

DB_TABLE_NAME_TO_ROWS_MISSING_WARNING = {}
for _db_table_name in DB_TABLE_NAMES:
    DB_TABLE_NAME_TO_ROWS_MISSING_WARNING[_db_table_name] = "Rows missing in {table}".format(table=_db_table_name)
DB_TABLE_NAME_TO_ROWS_MISSING_WARNING["calendar"] = "There are extra/missing rows in calendar that cannot be explained " \
                                                    "by dummy entries required by the calendar_dates table."

for dictionary in [DB_TABLE_NAME_TO_SOURCE_FILE, DB_TABLE_NAME_TO_ROWS_MISSING_WARNING]:
    for key in dictionary.keys():
        assert key in DB_TABLE_NAMES

for key in DB_TABLE_NAME_TO_FIELDS_WHERE_NULL_OK_BUT_WARN.keys():
    assert key in DB_TABLE_NAME_TO_FIELDS_WHERE_NULL_NOT_OK

#SOURCE_TABLE_NAMES = ['agency', 'routes', 'trips', 'calendar', 'calendar_dates', 'stop_times', 'stops', 'shapes']


class ImportValidator(object):

    def __init__(self, gtfssource, gtfs):
        """
        Parameters
        ----------
        gtfs_sources: list, string, dict
            list of paths to the strings, or a dictionary directly containing the gtfs data directly
        gtfs: gtfspy.gtfs.GTFS, or path to a relevant .sqlite GTFS database
        output_stream: something that one can write to
        """
        if isinstance(gtfssource, string_types + (dict,)):
            self.gtfs_sources = [gtfssource]
        else:
            assert isinstance(gtfssource, list)
            self.gtfs_sources = gtfssource
        assert len(self.gtfs_sources) > 0, "There needs to be some source files for validating an import"

        if not isinstance(gtfs, GTFS):
            self.gtfs = GTFS(gtfs)
        else:
            self.gtfs = gtfs

        self.location = self.gtfs.get_location_name()
        self.warnings_container = WarningsContainer()

    # def print_warnings(self):

    def get_warnings(self):
        self.warnings_container.clear()
        self._validate_table_row_counts()
        self._validate_no_null_values()
        self._validate_danglers()
        self.warnings_container.print_summary()
        return self.warnings_container

    def _validate_table_row_counts(self):
        """
        Imports source .txt files, checks row counts and then compares the rowcounts with the gtfsobject
        :return:
        """
        for db_table_name in DB_TABLE_NAME_TO_SOURCE_FILE.keys():
            table_name_source_file = DB_TABLE_NAME_TO_SOURCE_FILE[db_table_name]
            row_warning_str = DB_TABLE_NAME_TO_ROWS_MISSING_WARNING[db_table_name]

            # Row count in GTFS object:
            database_row_count = self.gtfs.get_row_count(db_table_name)

            # Row counts in source files:
            source_row_count = 0
            for gtfs_source in self.gtfs_sources:
                frequencies_in_source = source_csv_to_pandas(gtfs_source, 'frequencies.txt')
                try:
                    if table_name_source_file == 'trips' and not frequencies_in_source.empty:
                        source_row_count += self._frequency_generated_trips_rows(gtfs_source)

                    elif table_name_source_file == 'stop_times' and not frequencies_in_source.empty:
                        source_row_count += self._compute_number_of_frequency_generated_stop_times(gtfs_source)
                    else:
                        df = source_csv_to_pandas(gtfs_source, table_name_source_file)

                        source_row_count += len(df.index)
                except IOError as e:
                    if hasattr(e, "filename") and db_table_name in e.filename:
                        pass
                    else:
                        raise e


            if source_row_count == database_row_count:
                print("Row counts match for " + table_name_source_file + " between the source and database ("
                      + str(database_row_count) + ")")
            else:
                difference = database_row_count - source_row_count
                ('Row counts do not match for ' + str(table_name_source_file) + ': (source=' + str(source_row_count) +
                      ', database=' + str(database_row_count) + ")")
                if table_name_source_file == "calendar" and difference > 0:
                    query = "SELECT count(*) FROM (SELECT * FROM calendar ORDER BY service_I DESC LIMIT " \
                            + str(int(difference)) + \
                            ") WHERE start_date=end_date AND m=0 AND t=0 AND w=0 AND th=0 AND f=0 AND s=0 AND su=0"
                    number_of_entries_added_by_calendar_dates_loader = self.gtfs.execute_custom_query(query).fetchone()[
                        0]
                    if number_of_entries_added_by_calendar_dates_loader == difference:
                        print("    But don't worry, the extra entries seem to just dummy entries due to calendar_dates")
                    else:
                        print("    Reason for this is unknown.")
                        self.warnings_container.add_warning(self.location, row_warning_str, difference)
                else:
                    self.warnings_container.add_warning(self.location, row_warning_str, difference)


    def _validate_no_null_values(self):
        """
        Loads the tables from the gtfs object and counts the number of rows that have null values in
        fields that should not be null. Stores the number of null rows in warnings_container
        """
        for table in DB_TABLE_NAMES:
            null_not_ok_warning = "Null values in must-have columns in table {table}".format(table=table)
            null_warn_warning = "Null values in good-to-have columns in table {table}".format(table=table)
            null_not_ok_fields = DB_TABLE_NAME_TO_FIELDS_WHERE_NULL_NOT_OK[table]
            null_warn_fields = DB_TABLE_NAME_TO_FIELDS_WHERE_NULL_OK_BUT_WARN[table]

            # CW, TODO: make this validation source by source
            df = self.gtfs.get_table(table)

            for warning, fields in zip([null_not_ok_warning, null_warn_warning], [null_not_ok_fields, null_warn_fields]):
                null_unwanted_df = df[fields]
                rows_having_null = null_unwanted_df.isnull().any(1)
                if sum(rows_having_null) > 0:
                    print(rows_having_null)
                    rows_having_unwanted_null = df[rows_having_null.values]
                    self.warnings_container.add_warning(self.location, warning, value=rows_having_unwanted_null)


    def _validate_danglers(self):
        """
        Checks for rows that are not referenced in the the tables that should be linked

        stops <> stop_times using stop_I
        stop_times <> trips <> days, using trip_I
        trips <> routes, using route_I
        :return:
        """
        for query, warning in zip(DANGLER_QUERIES, DANGLER_WARNINGS):
            dangler_count = self.gtfs.execute_custom_query(query).fetchone()[0]
            if dangler_count > 0:
                print(str(dangler_count) + " " + warning)
                self.warnings_container.add_warning(self.location, warning, value=dangler_count)

    def _frequency_generated_trips_rows(self, gtfs_soure_path, return_df_freq=False):
        """
        This function calculates the equivalent rowcounts for trips when
        taking into account the generated rows in the gtfs object
        Parameters
        ----------
        gtfs_soure_path: path to the source file
        param txt: txt file in question
        :return: sum of all trips
        """
        df_freq = source_csv_to_pandas(gtfs_soure_path, 'frequencies')
        df_trips = source_csv_to_pandas(gtfs_soure_path, "trips")
        df_freq['n_trips'] = df_freq.apply(lambda row: len(range(str_time_to_day_seconds(row['start_time']),
                                                                 str_time_to_day_seconds(row['end_time']),
                                                                 row['headway_secs'])), axis=1)
        df_trips_freq = pd.merge(df_freq, df_trips, how='outer', on='trip_id')
        n_freq_generated_trips = int(df_trips_freq['n_trips'].fillna(1).sum(axis=0))
        if return_df_freq:
            return df_trips_freq
        else:
            return n_freq_generated_trips

    def _compute_number_of_frequency_generated_stop_times(self, gtfs_source_path):
        """
        Parameters
        ----------
        Same as for "_frequency_generated_trips_rows" but for stop times table
        gtfs_source_path:
        table_name:

        Return
        ------
        """
        df_freq = self._frequency_generated_trips_rows(gtfs_source_path, return_df_freq=True)
        df_stop_times = source_csv_to_pandas(gtfs_source_path, "stop_times")
        df_stop_freq = pd.merge(df_freq, df_stop_times, how='outer', on='trip_id')
        return int(df_stop_freq['n_trips'].fillna(1).sum(axis=0))

def main():
    pass

if __name__ == "__main__":
    main()
