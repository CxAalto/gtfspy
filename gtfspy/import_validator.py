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
"""

class ImportValidator(object):

    def __init__(self, gtfssource, gtfs):
        """
        Parameters
        ----------
        gtfs_sources: list of strings
        gtfs: GTFS, or path to a GTFS object
            A GTFS object
        """
        self.tablenames = ['agencies', 'routes', 'trips', 'calendar', 'calendar_dates', 'stop_times', 'stops', 'shapes']
        self.txtnames = ['agency', 'routes', 'trips', 'calendar', 'calendar_dates', 'stop_times', 'stops', 'shapes']
        self.df_freq_dict = {}
        if isinstance(gtfssource, string_types + (dict,)):
            _gtfs_sources = [gtfssource]
        else:
            assert isinstance(gtfssource, list)
            _gtfs_sources = gtfssource

        self.gtfs_sources = _gtfs_sources
        """
        for source in _gtfs_sources:
            # print(source)
            # dict input
            if isinstance(source, dict):
                self.gtfs_sources.append(source)
            # zipfile/dir input.
            elif isinstance(source, string_types):
                if os.path.isdir(source):
                    self.gtfs_sources.append(source)
                else:
                    z = zipfile.ZipFile(source, mode='r')
                    zip_commonprefix = os.path.commonprefix(z.namelist())
                    zip_source_datum = {
                        "zipfile": source,
                        "zip_commonprefix": zip_commonprefix
                    }
                    self.gtfs_sources.append(zip_source_datum)
"""
        if not isinstance(gtfs, GTFS):
            self.gtfs = GTFS(gtfs)
        else:
            self.gtfs = gtfs


    def txt_reader(self, source, table):
        """imports txt files as pandas df"""
        return txt_to_pandas(source, table)


    def db_table_counts(self, table):
        return self.gtfs.get_row_count(table)

    def source_gtfsobj_comparison(self):
        # print self.gtfs.execute_custom_query_pandas('SELECT * FROM trips ').to_string()
        row_count = {}
        for txt, table in zip(self.txtnames, self.tablenames):
            row_count[txt] = 0

            for gtfs_source in self.gtfs_sources:
                # print gtfs_source
                if txt == 'trips':
                    row_count[txt] = self.frequency_generated_trips(gtfs_source, txt)

                elif txt == 'stop_times':
                    row_count[txt] = self.frequency_generated_stop_times(gtfs_source, txt)

                else:
                    df = self.txt_reader(gtfs_source, txt)

                    row_count[txt] += len(df.index)

            # Result from GTFSobj:
            table_counts = self.db_table_counts(table)
            print('row count for source ' + str(txt) + ' is ' + str(row_count[txt]) + ' while the corresponding rowcount in gtfsobject is ' + str(table_counts))
            if not row_count[txt] == table_counts:
                print('Warning: difference in row_count for table ' + table)


    def null_counts_in_gtfs_obj(self):
        for table in self.tablenames:
            fields_where_null_ok = {
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

            df = self.gtfs.get_table(table)
            df.drop(fields_where_null_ok[table], inplace=True, axis=1)
            len_table = len(df.index)
            df.dropna(inplace=True, axis=0)
            len_non_null = len(df.index)
            nullrows = len_table - len_non_null
            if nullrows > 0:
                print('Warning: Null values detected in table ' + table)


    def frequency_generated_trips(self, source, txt):

        df_freq = txt_to_pandas(source, u'frequencies.txt')
        df_trips = txt_to_pandas(source, txt)
        df_freq['n_trips'] = df_freq.apply(lambda row: len(range(str_time_to_day_seconds(row['start_time']), str_time_to_day_seconds(row['end_time']), row['headway_secs'])), axis=1)
        self.df_freq_dict[source] = df_freq
        df_trips_freq = pd.merge(df_freq, df_trips, how='outer', on='trip_id')

        return int(df_trips_freq['n_trips'].fillna(1).sum(axis=0))

    def frequency_generated_stop_times(self, source, txt):

        df_stop_times = txt_to_pandas(source, txt)
        df_freq = self.df_freq_dict[source]
        df_stop_freq = pd.merge(df_freq, df_stop_times, how='outer', on='trip_id')

        return int(df_stop_freq['n_trips'].fillna(1).sum(axis=0))

def main():
    pass

if __name__ == "__main__":
    main()
