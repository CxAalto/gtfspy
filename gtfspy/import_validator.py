# reload(sys)
# -*- encoding: utf-8 -*-
import sys
from gtfspy.gtfs import GTFS
from six import string_types
import os
import zipfile

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
    tablenames= ['agencies', 'routes', 'trips', 'calendar', 'calendar_dates', 'stop_times', 'stops', 'shapes', 'feed_info']
    def __init__(self, gtfssource, gtfs):
        """
        Parameters
        ----------
        gtfs_sources: list of strings or
        gtfs: GTFS, or path to a GTFS object
            A GTFS object
        """
        if isinstance(gtfssource, string_types + (dict,)):
            _gtfs_sources = [gtfssource]
        else:
            assert isinstance(gtfssource, list)
            _gtfs_sources = gtfssource

        self.gtfs_sources = []

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

        if not isinstance(gtfs, GTFS):
            self.gtfs = GTFS(gtfs)
        else:
            self.gtfs = gtfs


    def txt_reader(self):
        """imports txt files as pandas df"""

        pass

    def db_table_counts(self, table):
        return self.gtfs.get_row_count(table)



def main():
    pass

if __name__ == "__main__":
    main()
