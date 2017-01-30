# reload(sys)
# -*- encoding: utf-8 -*-
import sys
from gtfspy.gtfs import GTFS
from six import string_types
import os
import zipfile
import pandas as pd
from util import txt_to_pandas
from util import str_time_to_day_seconds
from timetable_validator import WarningsContainer

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

# TODO: check that there are no unreferenced id pairs between tables:
# stops <> stop_times
# stop_times <> trips <> days
# trips <> routes
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
        self.warnings_container.print_summary()
        return self.warnings_container

    def _validate_table_counts(self):
        # print self.gtfs.execute_custom_query_pandas('SELECT * FROM trips ').to_string()
        row_count = {}
        for txt, table, row_warning in zip(SOURCE_TABLE_NAMES, DB_TABLE_NAMES, ROW_WARNINGS):
            row_count[txt] = 0

            for gtfs_source in self.gtfs_sources:
                # print gtfs_source
                try:
                    if txt == 'trips':
                        row_count[txt] += self._frequency_generated_trips(gtfs_source, txt)

                    elif txt == 'stop_times':
                        row_count[txt] += self._frequency_generated_stop_times(gtfs_source, txt)

                    else:
                        df = txt_to_pandas(gtfs_source, txt)

                        row_count[txt] += len(df.index)
                except IOError:
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
                self.warnings_container.add_warning(self.location, null_warning)



    def _frequency_generated_trips(self, source, txt):

        df_freq = txt_to_pandas(source, u'frequencies.txt')
        df_trips = txt_to_pandas(source, txt)
        df_freq['n_trips'] = df_freq.apply(lambda row: len(range(str_time_to_day_seconds(row['start_time']),
                                                                 str_time_to_day_seconds(row['end_time']),
                                                                 row['headway_secs'])), axis=1)
        self.df_freq_dict[source] = df_freq
        df_trips_freq = pd.merge(df_freq, df_trips, how='outer', on='trip_id')

        return int(df_trips_freq['n_trips'].fillna(1).sum(axis=0))

    def _frequency_generated_stop_times(self, source, txt):

        df_stop_times = txt_to_pandas(source, txt)
        df_freq = self.df_freq_dict[source]
        df_stop_freq = pd.merge(df_freq, df_stop_times, how='outer', on='trip_id')

        return int(df_stop_freq['n_trips'].fillna(1).sum(axis=0))

def main():
    pass

if __name__ == "__main__":
    main()
