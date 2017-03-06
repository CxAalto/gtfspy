# reload(sys)
# -*- encoding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function

import sys

"""
Importing GTFS into a sqlite database.

Entry point: see main part at the bottom and/or the import_gtfs function.
"""


import codecs
import csv
from datetime import datetime, timedelta
import os
import re
import sqlite3
import time
import zipfile
import pandas
from six import string_types

from gtfspy import stats
from gtfspy import util
from gtfspy.gtfs import GTFS
from gtfspy import calc_transfers


def decode_six(string):
    version = sys.version_info[0]
    if version == 2:
        return string.decode('utf-8')
    else:
        assert(isinstance(string, str))
        return string


class TableLoader(object):
    """Generic GTFS table loader.

    This is a generic table loader that can load any other GTFS table.
    The reason that this is a class is so that it can be subclassed
    and modified to easily import each other table without writing a
    bunch of redundant code.

    This class is just instantiated, and it does its stuff, and then
    it is destroyed.
    """
    mode = 'all'  # None or 'import' or 'index'.  "None" does everything.

    # The following properties need to be defined in a subclass. Examples here.
    # fname = 'routes.txt'
    # table = 'route'
    # tabledef = '(route TEXT PRIMARY KEY,
    #               agency_id TEXT,
    #               name TEXT,
    #               long_name TEXT,
    #               desc TEXT,
    #               type INT,
    #               url TEXT)'
    # Finally, a subclass needs to define these methods:
    # def gen_rows(self, reader):
    # def index(self):
    extra_keys = []
    extra_values = []
    is_zipfile = False
    table = ""  # e.g. stops for StopLoader

    def __init__(self, gtfssource=None, print_progress=True):
        """
        Parameters
        ----------
        gtfssource: str, dict, sqlite3.Connection, list
            str: path to GTFS directory or zipfile
            dict:
                dictionary of files to use to as the GTFS files.  This
                is mainly useful for testing, not for any normal use of
                GTFS.  For example, to provide an agency.txt file,
                do this:
                    d = {'agency.txt':
                             'agency_id, agency_name, agency_timezone,agency_url\n' \
                             '555,CompNet,Europe/Lala,http://x'
                          }
                Of course you probably wouldn't want all the string data
                inline like that. You can provide the data as a string or
                a file-like object (with a .read() attribute and line
                iteration).
            sqlite3.Connection:
            list: a list of the above elements to import (i.e. "merge") multiple GTFS feeds to the same database

        print_progress: boolean
            whether to print progress of the
        """
        # TODO: add support for sqlite3.Connection?
        if isinstance(gtfssource, string_types + (dict,)):
            _gtfs_sources = [gtfssource]
        else:
            assert isinstance(gtfssource, list)
            _gtfs_sources = gtfssource

        # whether to print progress of the import
        self.print_progress = print_progress

        self.gtfs_sources = []
        # map sources to "real"
        for source in _gtfs_sources:
            # print(source)
            # Deal with the case that gtfspath is actually a dict.
            if isinstance(source, dict):
                self.gtfs_sources.append(source)
            # zipfile input.
            # Warning: this keeps an open file handle as
            # long as this object exists, and it is duplicated across all
            # loaders. Maybe open it in the caller?
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

    # Methods that should be implemented by inheriting classes
    # when necessary.
    #
    #def post_import(self, cur):
    #def index(self, cur):

    # Methods for these classes:

    def exists(self):
        """Does this GTFS contain this file? (file specified by the class)"""
        # If file exists in one of the sources, return True
        for source in self.gtfs_sources:
            if isinstance(source, dict):
                # source can now be either a dict or a zipfile
                if self.fname in source:
                    if source[self.fname]:
                        return True
            # Handle zipfiles specially
            if "zipfile" in source:
                try:
                    Z = zipfile.ZipFile(source['zipfile'], mode='r')
                    Z.getinfo(os.path.join(source['zip_commonprefix'], self.fname))
                    return True
                # File does not exist in the zip archive
                except KeyError:
                    print(self.fname, ' missing in ', source)
                    continue
            # Normal filename
            if isinstance(source, string_types):
                if os.path.exists(os.path.join(source, self.fname)):
                    return True
        # the "file" was not found in any of the sources, return false
        return False

    def gen_rows0(self):
        """Iterate through all rows in all files file.

        The file is specified by the class - there is one class per
        table.  This opens the file, does basic sanitaition, and
        iterates over all rows as dictionaries, converted by
        csv.DictReader.  This function opens files from both .zip and
        raw directories.  The actual logic of converting all data to
        Python is done in the .gen_rows() method which must be
        defined in each subclass.
        """
        return self.gen_rows(*(self._get_csv_reader_generators()))

    def _get_csv_reader_generators(self):
        # This is a generator function that we use for importing.  It
        # makes a CSV reader that returns dictionaries, and passes
        # that to self.gen_rows that transform those CSV dictionaries
        # into the right form for importing into SQLite.  It is worth
        # pointing out that dictionaries are used everywhere here to
        # not have to depend on the particular ordering of fields, and
        # to make it easier to add more fields in the future.
        def _iter_file(file_obj):
            # Python2 csv reading is stupid when it comes to UTF8.
            # The input has to be strings. But some files have
            # the UTF8 byte order mark, which needs to be removed.
            # This hack removes the BOM from the start of any
            # line.
            version = sys.version_info[0]
            for line in file_obj:
                if isinstance(line, bytes):
                    yield line.lstrip(codecs.BOM_UTF8).decode("utf-8")
                elif version == 2:  # python2.x
                    if isinstance(line, str):
                        yield line
                    else:
                        yield line.lstrip(codecs.BOM_UTF8)
                else:
                    assert(isinstance(line, str))
                    yield line

        fs = []
        for source in self.gtfs_sources:
            f = []
            # Handle manually overridden files.
            if isinstance(source, dict):
                # source can now be either a dict or a zipfile
                if self.fname in source:
                    data_obj = source[self.fname]
                    if isinstance(data_obj, string_types):
                        f = data_obj.split("\n")
                    elif hasattr(data_obj, "read"):
                        # file-like object: use it as-is.
                        f = data_obj
                elif "zipfile" in source:
                    try:
                        Z = zipfile.ZipFile(source['zipfile'], mode='r')
                        # print(Z.namelist())
                        f = util.zip_open(Z, os.path.join(source['zip_commonprefix'], self.fname))
                    except KeyError:
                        pass
            elif isinstance(source, string_types):
                # now source is a directory
                try:
                    f = open(os.path.join(source, self.fname))
                # except OSError as e:
                except IOError as e:
                    f = []
            fs.append(f)

        # so far so good...
        csv_readers = [csv.DictReader(_iter_file(f)) for f in fs]
        csv_reader_generators = []
        for i, csv_reader in enumerate(csv_readers):
            try:
                csv_reader.fieldnames = [x.strip() for x in csv_reader.fieldnames]
                # Make a generator that strips the values in all fields before
                # passing it on.  GTFS standard requires that there be no
                # spare whitespace, but not all do it.  The csv module has a
                # `skipinitialspace` option, but let's make sure that we strip
                # it from both sides.
                # The following results in a generator, the complicated
                csv_reader_stripped = (dict((k, (v.strip() if v is not None else None))  # v is not always a string
                                            for k, v in row.items())
                                       for row in csv_reader)
                csv_reader_generators.append(csv_reader_stripped)
            except TypeError as e:
                if "NoneType" in str(e):
                    print(self.fname + " missing from feed " + str(i))
                    csv_reader_generators.append(iter(()))
                    #raise e here will make every multifeed download with incompatible number of tables fail
                else:
                    raise e
        prefixes = [u"feed_{i}_".format(i=i) for i in range(len(csv_reader_generators))]

        if len(prefixes) == 1:
            # no prefix for a single source feed
            prefixes = [u""]
        return csv_reader_generators, prefixes

    def gen_rows(self, csv_readers, prefixes):
        # to be overridden by Inherited classes
        pass

    def create_table(self, conn):
        """Make table definitions"""
        # Make cursor
        cur = conn.cursor()
        # Drop table if it already exists, to be recreated.  This
        # could in the future abort if table already exists, and not
        # recreate it from scratch.
        #cur.execute('''DROP TABLE IF EXISTS %s'''%self.table)
        #conn.commit()
        if self.tabledef is None:
            return
        if not self.tabledef.startswith('CREATE'):
            # "normal" table creation.
            cur.execute('CREATE TABLE IF NOT EXISTS %s %s'
                        % (self.table, self.tabledef)
                        )
        else:
            # When tabledef contains the full CREATE statement (for
            # virtual tables).
            cur.execute(self.tabledef)
        conn.commit()

    def insert_data(self, conn):
        """Load data from GTFS file into database"""
        cur = conn.cursor()
        # This is a bit hackish.  It is annoying to have to write the
        # INSERT statement yourself and keep it up to date with the
        # table rows.  This gets the first row, figures out the field
        # names from that, and then makes an INSERT statement like
        # "INSERT INTO table (col1, col2, ...) VALUES (:col1, :col2,
        # ...)".  The ":col1" is sqlite syntax for named value.

        csv_reader_generators, prefixes = self._get_csv_reader_generators()
        for csv_reader, prefix in zip(csv_reader_generators, prefixes):
            try:
                row = next(iter(self.gen_rows([csv_reader], [prefix])))
                fields = row.keys()
            except StopIteration:
                # The file has *only* a header and no data.
                # next(iter()) yields StopIteration and we can't
                # proceed.  Since there is nothing to import, just continue the loop
                print("Not importing %s into %s for %s" % (self.fname, self.table, prefix))
                continue
            stmt = '''INSERT INTO %s (%s) VALUES (%s)''' % (
                self.table,
                (', '.join([x for x in fields if x[0] != '_'] + self.extra_keys)),
                (', '.join([":" + x for x in fields if x[0] != '_'] + self.extra_values))
            )

            # This does the actual insertions.  Passed the INSERT
            # statement and then an iterator over dictionaries.  Each
            # dictionary is inserted.
            if self.print_progress:
                print('Importing %s into %s for %s' % (self.fname, self.table, prefix))
            # the first row was consumed by fetching the fields
            # (this could be optimized)
            from itertools import chain
            rows = chain([row], self.gen_rows([csv_reader], [prefix]))
            cur.executemany(stmt, rows)
            conn.commit()

            # This was used for debugging the missing service_I:
            # if self.__class__.__name__ == 'TripLoader': # and False:
                # for i in self.gen_rows([new_csv_readers[i]], [prefix]):
                # print(stmt)
                # rows = cur.execute('SELECT agency_id, trips.service_id FROM agencies, routes, trips
            # LEFT JOIN calendar ON(calendar.service_id=trips.service_id)
            # WHERE trips.route_I = routes.route_I and routes.agency_I = agencies.agency_I and trips.service_I is NULL
            # GROUP BY trips.service_id, agency_id').fetchall()
                # rows = cur.execute('SELECT distinct trips.service_id FROM trips
            # LEFT JOIN calendar ON(calendar.service_id=trips.service_id) WHERE trips.service_I is NULL').fetchall()

                # print('trips, etc', [description[0] for description in cur.description])
                # for i, row in enumerate(rows):
                    # print(row)
                    #if i == 100:
                        #exit(0)

                # rows = cur.execute('SELECT distinct service_id FROM calendar').fetchall()
                # print('calendar_columns',[description[0] for description in cur.description])
                # for row in rows:
                    # print(row)

    def run_post_import(self, conn):
        if self.print_progress:
            print('Post-import %s into %s' % (self.fname, self.table))
        cur = conn.cursor()
        self.post_import(cur)
        conn.commit()

    def create_index(self, conn):
        if not hasattr(self, 'index'):
            return
        cur = conn.cursor()
        if self.print_progress:
            print('Indexing %s' % (self.table,))
        self.index(cur)
        conn.commit()

    def import_(self, conn):
        """Do the actual import. Copy data and store in connection object.

        This function:
        - Creates the tables
        - Imports data (using self.gen_rows)
        - Run any post_import hooks.
        - Creates any indexs
        - Does *not* run self.make_views - those must be done
          after all tables are loaded.
        """
        if self.print_progress:
            print('Beginning', self.__class__.__name__)
        # what is this mystical self._conn ?
        self._conn = conn

        self.create_table(conn)
        # This does insertions
        if self.mode in ('all', 'import') and self.fname and self.exists() and self.table not in ignore_tables:
            self.insert_data(conn)
        # This makes indexes in the DB.
        if self.mode in ('all', 'index') and hasattr(self, 'index'):
            self.create_index(conn)
        # Any post-processing to be done after the full import.
        if self.mode in ('all', 'import') and hasattr(self, 'post_import'):
            self.run_post_import(conn)
        # Commit it all
        conn.commit()

    @classmethod
    def make_views(cls, conn):
        """The make views should be run after all tables imported."""
        pass

    copy_where = ''

    @classmethod
    def copy(cls, conn, **where):
        """Copy data from one table to another while filtering data at the same time

        Parameters
        ----------
        conn: sqlite3 DB connection.  It must have a second database
            attached as "other".
        **where : keyword arguments
            specifying (start_ut and end_ut for filtering, see the copy_where clause in the subclasses)
        """
        cur = conn.cursor()
        if where and cls.copy_where:
            copy_where = cls.copy_where.format(**where)
            # print(copy_where)
        else:
            copy_where = ''
        cur.execute('INSERT INTO %s '
                    'SELECT * FROM source.%s %s' % (cls.table, cls.table, copy_where))

    @classmethod
    def post_import_round2(cls, conn):
        pass


class StopLoader(TableLoader):
    # This class is documented to explain what it does, others are not.
    # Metadata needed to create table.  GTFS filename, table name, and
    # the CREATE TABLE syntax (last part only).
    fname = 'stops.txt'
    table = 'stops'
    tabledef = '''(stop_I INTEGER PRIMARY KEY, stop_id TEXT UNIQUE NOT NULL, code TEXT, name TEXT, desc TEXT, lat REAL, lon REAL, parent_I INT, location_type INT, wheelchair_boarding BOOL, self_or_parent_I INT)'''

    # stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,wheelchair_boarding
    # 1010103,2008,"Kirkkokatu","Mariankatu 13",60.171263,24.956605,1,http://aikataulut.hsl.fi/pysakit/fi/1010103.html,0, ,2
    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                # and transform the "row" dictionary into a new
                # dictionary, which is yielded.  There can be different
                # transformations on here, as needed.
                yield dict(
                    stop_id       = prefix + decode_six(row['stop_id']),
                    code          = decode_six(row['stop_code']) if 'stop_code' in row else None,
                    name          = decode_six(row['stop_name']),
                    desc          = decode_six(row['stop_desc']) if 'stop_desc' in row else None,
                    lat           = float(row['stop_lat']),
                    lon           = float(row['stop_lon']),
                    _parent_id    = prefix + decode_six(row['parent_station']) if row.get('parent_station','') else None,
                    location_type = int(row['location_type']) if row.get('location_type') else None,
                    wheelchair_boarding = int(row['wheelchair_boarding']) if row.get('wheelchair_boarding','') else None,
                )

    def post_import(self, cur):
        # if parent_id, set  also parent_I:
        # :_parent_id stands for a named parameter _parent_id
        # inputted through a dictionary in cur.executemany
        stmt = ('UPDATE %s SET parent_I=CASE WHEN (:_parent_id IS NOT "") THEN '
                '(SELECT stop_I FROM %s WHERE stop_id=:_parent_id) END '
                'WHERE stop_id=:stop_id') % (self.table, self.table)
        if self.exists():
            cur.executemany(stmt, self.gen_rows0())
        stmt = 'UPDATE %s ' \
               'SET self_or_parent_I=coalesce(parent_I, stop_I)' % self.table
        cur.execute(stmt)

    def index(self, cur):
        # Make indexes/ views as needed.
        #cur.execute('CREATE INDEX IF NOT EXISTS idx_stop_sid ON stop (stop_id)')
        #cur.execute('CREATE INDEX IF NOT EXISTS idx_stops_pid_sid ON stops (parent_id, stop_I)')
        #conn.commit()
        pass


# View for adjacent stops
#        cur.execute('''CREATE VIEW IF NOT EXISTS view_stop_groups \
#AS SELECT stop.stop_I, other_stop.stop_I other_stop_I \
#FROM stop join stop other_stop ON (other_stop.parent_I=stop.parent_I) \
#WHERE stop.stop_I!=other_stop.stop_I''')

class StopRtreeLoader(TableLoader):
    fname = None
    table = 'stops_rtree'
    tabledef = ('CREATE VIRTUAL TABLE IF NOT EXISTS stops_rtree USING '
                'rtree(stop_I, lat, lat2, lon, lon2)')

    @classmethod
    def post_import(self, cur):
        cur.execute('INSERT INTO stops_rtree SELECT stop_I, lat, lat, lon, lon FROM stops')


class RouteLoader(TableLoader):
    fname = 'routes.txt'
    table = 'routes'
    tabledef = '(route_I INTEGER PRIMARY KEY, ' \
               'route_id TEXT UNIQUE NOT NULL, ' \
               'agency_I INT, ' \
               'name TEXT, ' \
               'long_name TEXT, ' \
               'desc TEXT, ' \
               'type INT, ' \
               'url TEXT, ' \
               'color TEXT, ' \
               'text_color TEXT' \
               ')'
    extra_keys = ['agency_I', ]
    extra_values = ['(SELECT agency_I FROM agencies WHERE agency_id=:_agency_id )',
                    ]

    # route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url
    # 1001,HSL,1,Kauppatori - Kapyla,0,http://aikataulut.hsl.fi/linjat/fi/h1_1a.html
    def gen_rows(self, readers, prefixes):
        from gtfspy import extended_route_types
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                yield dict(
                    route_id      = prefix + decode_six(row['route_id']),
                    _agency_id    = prefix + decode_six(row['agency_id']) if 'agency_id' in row else None,
                    name          = decode_six(row['route_short_name']),
                    long_name     = decode_six(row['route_long_name']),
                    desc          = decode_six(row['route_desc']) if 'route_desc' in row else None,
                    type          = extended_route_types.ROUTE_TYPE_CONVERSION[int(row['route_type'])],
                    url           = decode_six(row['route_url']) if 'route_url' in row else None,
                    color         = decode_six(row['route_color']) if 'route_color' in row else None,
                    text_color    = decode_six(row['route_text_color']) if 'route_text_color' in row else None,
                )

    @classmethod
    def index(cls, cur):
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_rid ON route (route_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_route_name ON routes (name)')


class TripLoader(TableLoader):
    fname = 'trips.txt'
    table = 'trips'
    # service_I INT NOT NULL
    tabledef = ('(trip_I INTEGER PRIMARY KEY, trip_id TEXT UNIQUE NOT NULL, '
                'route_I INT, service_I INT, direction_id TEXT, shape_id TEXT, '
                'headsign TEXT, '
                'start_time_ds INT, end_time_ds INT)')
    extra_keys = ['route_I', 'service_I' ] #'shape_I']
    extra_values = ['(SELECT route_I FROM routes WHERE route_id=:_route_id )',
                    '(SELECT service_I FROM calendar WHERE service_id=:_service_id )',
                    #'(SELECT shape_I FROM shapes WHERE shape_id=:_shape_id )'
                    ]

    # route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,bikes_allowed
    # 1001,1001_20150424_20150426_Ke,1001_20150424_Ke_1_0953,"Kapyla",0,1001_20140811_1,1,2
    def gen_rows(self, readers, prefixes):
        #try:
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                    yield dict(
                        _route_id     = prefix + decode_six(row['route_id']),
                        _service_id   = prefix + decode_six(row['service_id']),
                        trip_id       = prefix + decode_six(row['trip_id']),
                        direction_id  = decode_six(row['direction_id']) if row.get('direction_id','') else None,
                        shape_id      = prefix + decode_six(row['shape_id']) if row.get('shape_id','') else None,
                        headsign      = decode_six(row['trip_headsign']) if 'trip_headsign' in row else None,
                        )
        #except:
            #print(row)

    @classmethod
    def index(cls, cur):
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_trips_tid ON trips (trip_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_trips_svid ON trips (service_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_trips_rid ON trips (route_I)')

    @classmethod
    def calculate_times_ds(cls, conn):
        cur0 = conn.cursor()
        cur = conn.cursor()
        cur0.execute('''SELECT trip_I, min(dep_time), max(arr_time)
                       FROM trips JOIN stop_times USING (trip_I)
                       GROUP BY trip_I''')

        def iter_rows(cur0):
            for row in cur0:
                if row[1]:
                    st = row[1].split(':')
                    start_time_ds = int(st[0]) * 3600 + int(st[1]) * 60 + int(st[2])
                else:
                    start_time_ds = None
                if row[2]:
                    et = row[2].split(':')
                    end_time_ds   = int(et[0]) * 3600 + int(et[1]) * 60 + int(et[2])
                else:
                    end_time_ds = None
                yield start_time_ds, end_time_ds, row[0]

        cur.executemany('''UPDATE trips SET start_time_ds=?, end_time_ds=? WHERE trip_I=?''',
                        iter_rows(cur0))
        conn.commit()

    def post_import_round2(self, conn):
        self.calculate_times_ds(conn)

    # This has now been moved to DayTripsMaterializer, but is left
    # here in case we someday want to make DayTripsMaterializer
    # optional.
    #@classmethod
    #def make_views(cls, conn):
    #    conn.execute('DROP VIEW IF EXISTS main.day_trips')
    #    conn.execute('CREATE VIEW day_trips AS   '
    #                 'SELECT trips.*, days.*, '
    #                 'days.day_start_ut+trips.start_time_ds AS start_time_ut, '
    #                 'days.day_start_ut+trips.end_time_ds AS end_time_ut   '
    #                 'FROM days JOIN trips USING (trip_I);')
    #    conn.commit()


class StopTimesLoader(TableLoader):
    fname = 'stop_times.txt'
    table = 'stop_times'
    tabledef = ('(stop_I INT, trip_I INT, arr_time TEXT, dep_time TEXT, '
                'seq INT, arr_time_hour INT, shape_break INT, '
                'arr_time_ds INT, dep_time_ds INT)')
    extra_keys = ['stop_I',
                  'trip_I',
                  'arr_time_ds',
                  'dep_time_ds',
                  ]
    extra_values = ['(SELECT stop_I FROM stops WHERE stop_id=:_stop_id )',
                    '(SELECT trip_I FROM trips WHERE trip_id=:_trip_id )',
                    '(substr(:arr_time,-8,2)*3600 + substr(:arr_time,-5,2)*60 + substr(:arr_time,-2))',
                    '(substr(:dep_time,-8,2)*3600 + substr(:dep_time,-5,2)*60 + substr(:dep_time,-2))',
                    ]

    # trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled
    # 1001_20150424_Ke_1_0953,09:53:00,09:53:00,1030423,1,,0,1,0.0000
    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                yield dict(
                    _stop_id      = prefix + decode_six(row['stop_id']),
                    _trip_id      = prefix + decode_six(row['trip_id']),
                    arr_time      = row['arrival_time'],
                    dep_time      = row['departure_time'],
                    seq           = int(row['stop_sequence']),
                )

    def post_import(self, cur):
        # The following makes an arr_time_hour column that has an
        # integer of the arrival time hour.  Conversion to integer is
        # done in the sqlite engine, since the column affinity is
        # declared to be INT.
        cur.execute('UPDATE stop_times SET arr_time_hour = substr(arr_time, -8, 2)')
        calculate_trip_shape_breakpoints(self._conn)

        # Resequence seq value to increments of 1 starting from 1
        rows = cur.execute('SELECT ROWID, trip_I, seq FROM stop_times ORDER BY trip_I, seq').fetchall()


        old_trip_I = ''
        for row in rows:
            rowid = row[0]
            trip_I = row[1]
            seq = row[2]

            if old_trip_I != trip_I:
                correct_seq = 1
            if seq != correct_seq:
                cur.execute('UPDATE stop_times SET seq = ? WHERE ROWID = ?', (correct_seq, rowid))
            old_trip_I = trip_I
            correct_seq += 1



    @classmethod
    def index(cls, cur):
        cur.execute('CREATE INDEX IF NOT EXISTS idx_stop_times_tid_seq ON stop_times (trip_I, seq)')
        # Do *not* use this index, use the one below
        #cur.execute('CREATE INDEX idx_stop_times_tid_ath ON stop_times (trip_id, arr_time_hour)')
        # This is used for the stop frequencies analysis.
        #cur.execute('CREATE INDEX idx_stop_times_tid_ath_sid ON stop_times (trip_I, arr_time_hour, stop_id)')
            # ^-- much slower than the next index.
        cur.execute('CREATE INDEX idx_stop_times_ath_tid_sid ON stop_times (arr_time_hour, trip_I, stop_I)')

    # This has now been moved to DayTripsMaterializer, but is left
    # here in case we someday want to make DayTripsMaterializer
    # optional.
    #def make_views(self, conn):
    #    conn.execute('DROP VIEW IF EXISTS main.day_stop_times')
    #    conn.execute('CREATE VIEW day_stop_times AS   '
    #                 'SELECT stop_times.*, trips.*, days.*, '
    #                 'days.day_start_ut+trips.start_time_ds AS start_time_ut, '
    #                 'days.day_start_ut+trips.end_time_ds AS end_time_ut, '
    #                 'days.day_start_ut+stop_times.arr_time_ds AS arr_time_ut, '
    #                 'days.day_start_ut+stop_times.dep_time_ds AS dep_time_ut   '
    #                 'FROM days '
    #                 'JOIN trips USING (trip_I) '
    #                 'JOIN stop_times USING (trip_I)')
    #    conn.commit()


class DayTripsMaterializer(TableLoader):
    """Make the table day_trips with (date, trip_I, start, end, day_start_ut).

    This replaces the old day_trips view.  This allows direct querying
    on the start_time_ut and end_time_ut, at the cost that this table is
    now O(days * trips).  This makes the following things:

    day_trips2: The actual table
    day_trips: Replacement for the old day_trips view.  day_trips2+trips
    day_stop_times: day_trips2+trips+stop_times
    """
    fname = None
    table = 'day_trips2'
    tabledef = ('(date TEXT, '
                'trip_I INT, '
                'start_time_ut INT, '
                'end_time_ut INT, '
                'day_start_ut INT)')
    copy_where = 'WHERE  {start_ut} < end_time_ut  AND  start_time_ut < {end_ut}'

    @classmethod
    def post_import_round2(cls, conn):
        cur = conn.cursor()
        cur.execute('INSERT INTO day_trips2 '
                    'SELECT date, trip_I, '
                    'days.day_start_ut+trips.start_time_ds AS start_time_ut, '
                    'days.day_start_ut+trips.end_time_ds AS end_time_ut, '
                    'day_start_ut '
                    'FROM days '
                    'JOIN trips USING (trip_I)')
        conn.commit()

    def index(cls, cur):
        cur.execute('CREATE INDEX IF NOT EXISTS idx_day_trips2_tid '
                    'ON day_trips2 (trip_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_day_trips2_d '
                    'ON day_trips2 (date)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_day_trips2_stut_etut '
                    'ON day_trips2 (start_time_ut, end_time_ut)')
        # This index may not be needed anymore.
        cur.execute('CREATE INDEX IF NOT EXISTS idx_day_trips2_dsut '
                    'ON day_trips2 (day_start_ut)')

    @classmethod
    def make_views(cls, conn):
        """Create day_trips and day_stop_times views.

        day_trips:  day_trips2 x trips  = days x trips
        day_stop_times: day_trips2 x trips x stop_times = days x trips x stop_times
        """
        conn.execute('DROP VIEW IF EXISTS main.day_trips')
        conn.execute('CREATE VIEW day_trips AS   '
                     'SELECT day_trips2.*, trips.* '
                     #'days.day_start_ut+trips.start_time_ds AS start_time_ut, '
                     #'days.day_start_ut+trips.end_time_ds AS end_time_ut   '
                     'FROM day_trips2 JOIN trips USING (trip_I);')
        conn.commit()

        conn.execute('DROP VIEW IF EXISTS main.day_stop_times')
        conn.execute('CREATE VIEW day_stop_times AS   '
                     'SELECT day_trips2.*, trips.*, stop_times.*, '
                     #'days.day_start_ut+trips.start_time_ds AS start_time_ut, '
                     #'days.day_start_ut+trips.end_time_ds AS end_time_ut, '
                     'day_trips2.day_start_ut+stop_times.arr_time_ds AS arr_time_ut, '
                     'day_trips2.day_start_ut+stop_times.dep_time_ds AS dep_time_ut   '
                     'FROM day_trips2 '
                     'JOIN trips USING (trip_I) '
                     'JOIN stop_times USING (trip_I)')
        conn.commit()


class ShapeLoader(TableLoader):
    fname = 'shapes.txt'
    table = 'shapes'
    tabledef = '(shape_id TEXT, lat REAL, lon REAL, seq INT, d INT)'

    # shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
    # 1001_20140811_1,60.167430,24.951684,1
    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                yield dict(
                    shape_id      = prefix + decode_six(row['shape_id']),
                    lat           = float(row['shape_pt_lat']),
                    lon           = float(row['shape_pt_lon']),
                    seq           = int(row['shape_pt_sequence'])
                )

    @classmethod
    def index(cls, cur):
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_shapes_shid ON shapes (shape_id)')
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_shapes_id_seq ON shapes (shape_I, seq)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_shapes_id_seq ON shapes (shape_id, seq)')

    @classmethod
    def post_import(cls, cur):
        from gtfspy import shapes
        cur.execute('SELECT DISTINCT shape_id FROM shapes')
        shape_ids = tuple(x[0] for x in cur)

        # print "Renumbering sequences to start from 0 and Calculating shape cumulative distances"
        for shape_id in shape_ids:
            rows = cur.execute("SELECT shape_id, seq "
                               "FROM shapes "
                               "WHERE shape_id=? "
                               "ORDER BY seq", (shape_id,)
            ).fetchall()
            cur.executemany("UPDATE shapes SET seq=? "
                            "WHERE shape_id=? AND seq=?",
                            ( (i, shape_id, seq)
                             for i, (shape_id, seq) in enumerate(rows))
                            )

        for shape_id in shape_ids:
            shape_points = shapes.get_shape_points(cur, shape_id)
            shapes.gen_cumulative_distances(shape_points)

            cur.executemany('UPDATE shapes SET d=? '
                            'WHERE shape_id=? AND seq=? ',
                            ((pt['d'], shape_id, pt['seq'])
                             for pt in shape_points))


class CalendarLoader(TableLoader):
    fname = 'calendar.txt'
    table = 'calendar'
    tabledef = '(service_I INTEGER PRIMARY KEY, service_id TEXT UNIQUE NOT NULL, m INT, t INT, w INT, th INT, f INT, s INT, su INT, start_date TEXT, end_date TEXT)'
    copy_where = ("WHERE  date({start_ut}, 'unixepoch', 'localtime') < end_date "
                  "AND  start_date < date({end_ut}, 'unixepoch', 'localtime')")

    # service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
    # 1001_20150810_20151014_Ke,0,0,1,0,0,0,0,20150810,20151014
    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                # print row
                start = row['start_date']
                end = row['end_date']
                yield dict(
                    service_id    = prefix + decode_six(row['service_id']),
                    m             = int(row['monday']),
                    t             = int(row['tuesday']),
                    w             = int(row['wednesday']),
                    th            = int(row['thursday']),
                    f             = int(row['friday']),
                    s             = int(row['saturday']),
                    su            = int(row['sunday']),
                    start_date    = '%s-%s-%s' % (start[:4], start[4:6], start[6:8]),
                    end_date      = '%s-%s-%s' % (end[:4], end[4:6], end[6:8]),
                )

    @classmethod
    def index(cls, cur):
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_svid ON calendar (service_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_s_e ON calendar (start_date, end_date)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_m  ON calendar (m)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_t  ON calendar (t)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_w  ON calendar (w)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_th ON calendar (th)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_f  ON calendar (f)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_s  ON calendar (s)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_su ON calendar (su)')


class CalendarDatesLoader(TableLoader):
    fname = 'calendar_dates.txt'
    table = 'calendar_dates'
    tabledef = '(service_I INTEGER NOT NULL, date TEXT, exception_type INT)'
    copy_where = ("WHERE  date({start_ut}, 'unixepoch', 'localtime') <= date "
                  "AND  date < date({end_ut}, 'unixepoch', 'localtime')")

    def gen_rows(self, readers, prefixes):
        conn = self._conn
        cur = conn.cursor()
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                date = row['date']
                date_str = '%s-%s-%s' % (date[:4], date[4:6], date[6:8])
                service_id = prefix+row['service_id']
                # We need to find the service_I of this.  To do this we
                # need to check the calendar table, since that (and only
                # that) is the absolute list of service_ids.
                service_I = cur.execute(
                    'SELECT service_I FROM calendar WHERE service_id=?',
                    (decode_six(service_id),)).fetchone()
                if service_I is None:
                    # We have to add a new fake row in order to get a
                    # service_I.  calendar is *the* authoritative source
                    # for service_I:s.
                    cur.execute('INSERT INTO calendar '
                                '(service_id, m,t,w,th,f,s,su, start_date,end_date)'
                                'VALUES (?, 0,0,0,0,0,0,0, ?,?)',
                                (decode_six(service_id), date_str, date_str)
                                )
                    service_I = cur.execute(
                        'SELECT service_I FROM calendar WHERE service_id=?',
                        (decode_six(service_id),)).fetchone()
                service_I = service_I[0]  # row tuple -> int

                yield dict(
                    service_I     = int(service_I),
                    date          = date_str,
                    exception_type= int(row['exception_type']),
                )


class AgencyLoader(TableLoader):
    fname = 'agency.txt'
    table = 'agencies'
    tabledef = ('(agency_I INTEGER PRIMARY KEY, agency_id TEXT UNIQUE NOT NULL, '
                'name TEXT, url TEXT, timezone TEXT, lang TEXT, phone TEXT)')

    # shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
    # 1001_20140811_1,60.167430,24.951684,1
    def gen_rows(self, readers, prefixes):

        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                yield dict(
                    agency_id     =prefix + decode_six(row.get('agency_id', '1')),
                    name          = decode_six(row['agency_name']),
                    timezone      = decode_six(row['agency_timezone']),
                    url           = decode_six(row['agency_url']),
                    lang          = decode_six(row['agency_lang']) if 'agency_lang' in row else None,
                    phone         = decode_six(row['agency_phone']) if 'agency_phone' in row else None,
                )

    def post_import(self, cur):
        TZs = cur.execute('SELECT DISTINCT timezone FROM agencies').fetchall()
        if len(TZs) == 0:
            raise ValueError("Error: no timezones defined in sources: %s" % self.gtfs_sources)
        elif len(TZs) > 1:
            first_tz = TZs[0][0]
            import pytz
            for tz in TZs[1:]:
                generic_date = datetime(2009, 9, 1)
                ftz = pytz.timezone(first_tz).utcoffset(generic_date, is_dst=True)
                ctz = pytz.timezone(tz[0]).utcoffset(generic_date, is_dst=True)
                if not str(ftz) == str(ctz):
                    raise ValueError("Error: multiple timezones defined in sources:: %s" % self.gtfs_sources)
        TZ = TZs[0][0]
        os.environ['TZ'] = TZ
        time.tzset()  # Cause C-library functions to notice the update.

    def index(self, cur):
        pass


class TransfersLoader(TableLoader):
    """Loader to calculate transfer distances.

    transfer_type, from GTFS spec:
      0/null: recommended transfer point
      1: timed transfer
      2: minimum amount of time
      3: transfers not possible

    """
    # This loader is special.  calc_transfers creates the table there,
    # too.  We put a tabledef here so that copy() will work.
    fname = 'transfers.txt'
    table = 'transfers'
    # TODO: this is copy-pasted from calc_transfers.
    tabledef = ('(from_stop_I INT, '
                'to_stop_I INT, '
                'transfer_type INT, '
                'min_transfer_time INT'
                ')')
    extra_keys = ['from_stop_I',
                  'to_stop_I',
                  ]
    extra_values = ['(SELECT stop_I FROM stops WHERE stop_id=:_from_stop_id)',
                    '(SELECT stop_I FROM stops WHERE stop_id=:_to_stop_id)',
                    ]

    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                yield dict(
                    _from_stop_id     = prefix + decode_six(row['from_stop_id']).strip(),
                    _to_stop_id       = prefix + decode_six(row['to_stop_id']).strip(),
                    transfer_type     = int(row['transfer_type']),
                    min_transfer_time = int(row['min_transfer_time'])
                                        if ('min_transfer_time' in row
                                        and (row.get('min_transfer_time').strip()) )
                                        else None
            )



class FrequenciesLoader(TableLoader):
    """Load the general frequency table."""
    # This loader is special.  calc_transfers creates the table there,
    # too.  We put a tabledef here so that copy() will work.
    fname = 'frequencies.txt'
    table = 'frequencies'

    # TODO: this is copy-pasted from calc_transfers.
    tabledef = (u'(trip_I INT, '
                u'start_time TEXT, '
                u'end_time TEXT, '
                u'headway_secs INT,'
                u'exact_times INT, '
                u'start_time_ds INT, '
                u'end_time_ds INT'
                u')')
    extra_keys = [u'trip_I',
                  u'start_time_ds',
                  u'end_time_ds',
                  ]
    extra_values = [u'(SELECT trip_I FROM trips WHERE trip_id=:_trip_id )',
                    '(substr(:start_time,-8,2)*3600 + substr(:start_time,-5,2)*60 + substr(:start_time,-2))',
                    '(substr(:end_time,-8,2)*3600 + substr(:end_time,-5,2)*60 + substr(:end_time,-2))',
                    ]

    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                yield dict(
                    _trip_id = prefix + decode_six(row['trip_id']),
                    start_time = row['start_time'],
                    end_time = row['end_time'],
                    headway_secs = int(row['headway_secs']),
                    exact_times = int(row['exact_times']) if 'exact_times' in row and row['exact_times'].isdigit() else 0
                )

    def post_import(self, cur):
        # For each (start_time_dependent) trip_I in frequencies.txt
        conn = self._conn
        frequencies_df = pandas.read_sql("SELECT * FROM " + self.table, conn)
        trips_df = pandas.read_sql("SELECT * FROM " + "trips", conn)
        calendar_df = pandas.read_sql("SELECT * FROM " + "calendar", conn)

        for freq_tuple in frequencies_df.itertuples():
            trip_data = pandas.read_sql_query("SELECT * FROM trips WHERE trip_I= " + str(int(freq_tuple.trip_I)), conn)
            assert len(trip_data) == 1
            trip_data = list(trip_data.itertuples())[0]
            freq_start_time_ds = freq_tuple.start_time_ds
            freq_end_time_ds = freq_tuple.end_time_ds
            trip_duration = cur.execute("SELECT max(arr_time_ds) - min(dep_time_ds) "
                                        "FROM stop_times "
                                        "WHERE trip_I={trip_I}".format(trip_I=str(int(freq_tuple.trip_I)))
                                        ).fetchone()[0]
            if trip_duration is None:
                raise ValueError("Stop times for frequency trip " + trip_data.trip_id + " are not properly defined")
            headway = freq_tuple.headway_secs
            #print trip_data.trip_I
            sql = "SELECT * FROM stop_times WHERE trip_I=" + str(trip_data.trip_I) + " ORDER BY seq"
            stop_time_data = pandas.read_sql_query(sql, conn)

            start_times_ds = range(freq_start_time_ds, freq_end_time_ds, headway)
            for i, start_time in enumerate(start_times_ds):
                trip_id = trip_data.trip_id + u"_freq_" + str(start_time)
                route_I = trip_data.route_I
                service_I = trip_data.service_I

                shape_id = trip_data.shape_id
                direction_id = trip_data.direction_id
                headsign = trip_data.headsign
                end_time_ds = start_time + trip_duration

                # insert these into trips
                query = "INSERT INTO trips (trip_id, route_I, service_I, shape_id, direction_id, " \
                            "headsign, start_time_ds, end_time_ds)" \
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

                params = [trip_id, int(route_I), int(service_I), shape_id, direction_id, headsign, int(start_time), int(end_time_ds)]
                cur.execute(query, params)

                query = "SELECT trip_I FROM trips WHERE trip_id='{trip_id}'".format(trip_id=trip_id)
                trip_I = cur.execute(query).fetchone()[0]

                # insert into stop_times
                # TODO! get the original data
                dep_times_ds = stop_time_data['dep_time_ds']
                dep_times_ds = dep_times_ds - min(dep_times_ds) + start_time
                arr_times_ds = stop_time_data['arr_time_ds']
                arr_times_ds = arr_times_ds - min(arr_times_ds) + start_time
                shape_breaks = stop_time_data['shape_break']
                stop_Is = stop_time_data['stop_I']
                for seq, (dep_time_ds, arr_time_ds, shape_break, stop_I) in enumerate(zip(dep_times_ds,
                                                                                          arr_times_ds,
                                                                                          shape_breaks,
                                                                                          stop_Is)):
                    arr_time_hour = int(arr_time_ds // 3600)
                    query = "INSERT INTO stop_times (trip_I, stop_I, arr_time, " \
                            "dep_time, seq, arr_time_hour, shape_break, arr_time_ds, dep_time_ds) " \
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    arr_time = util.day_seconds_to_str_time(arr_time_ds)
                    dep_time = util.day_seconds_to_str_time(dep_time_ds)
                    cur.execute(query, (int(trip_I), int(stop_I), arr_time, dep_time, int(seq + 1),
                                        int(arr_time_hour), shape_break, int(arr_time_ds), int(dep_time_ds)))

        trip_Is = frequencies_df['trip_I'].unique()
        for trip_I in trip_Is:
            for table in ["trips", "stop_times"]:
                cur.execute("DELETE FROM {table} WHERE trip_I={trip_I}".format(table=table, trip_I=trip_I))


class FeedInfoLoader(TableLoader):

    """feed_info.txt: various feed metadata"""
    fname = 'feed_info.txt'
    table = 'feed_info'
    tabledef = ('(feed_publisher_name TEXT, '
                'feed_publisher_url TEXT, '
                'feed_lang TEXT, '
                'feed_start_date TEXT, '
                'feed_end_date TEXT, '
                'feed_version TEXT, '
                'feed_id TEXT) ')

    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                start = row['feed_start_date'] if 'feed_start_date' in row else None
                end   = row['feed_end_date']   if 'feed_end_date'   in row else None
                yield dict(
                    feed_publisher_name = decode_six(row['feed_publisher_name']) if 'feed_publisher_name' in row else None,
                    feed_publisher_url  = decode_six(row['feed_publisher_url'])  if 'feed_publisher_url'  in row else None,
                    feed_lang           = decode_six(row['feed_lang'])           if 'feed_lang'           in row else None,
                    feed_start_date     = '%s-%s-%s'%(start[:4], start[4:6], start[6:8])  if start else None,
                    feed_end_date       = '%s-%s-%s'%(end[:4], end[4:6], end[6:8])        if end   else None,
                    feed_version        = decode_six(row['feed_version'])        if 'feed_version'        in row else None,
                    feed_id             = prefix[:-1] if len(prefix) > 0 else prefix
                )

    def post_import2(self, conn):
        # TODO! Something whould be done with this! Multiple feeds are possible, currently only selects one row for all feeds
        G = GTFS(conn)
        for name in ['feed_publisher_name',
                     'feed_publisher_url',
                     'feed_lang',
                     'feed_start_date',
                     'feed_end_date',
                     'feed_version']:
                value = conn.execute('SELECT %s FROM feed_info' % name).fetchone()[0]
                if value:
                    G.meta['feed_info_' + name] = value


class DayLoader(TableLoader):
    # Note: calendar and calendar_dates should have been imported before
    # importing with DayLoader
    fname = None
    table = 'days'
    tabledef = '(date TEXT, day_start_ut INT, trip_I INT)'
    copy_where = "WHERE  {start_ut} <= day_start_ut  AND  day_start_ut < {end_ut}"

    def post_import(self, cur):
        days = []
        # This index is important here, but no where else, and not for
        # future processing.  So, create it here, delete it at the end
        # of the function.  If this index was important, it could be
        # moved to CalendarDatesLoader.
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_dates_sid ON calendar_dates (service_I)')

        cur.execute('SELECT * FROM calendar')
        colnames = cur.description
        cur2 = self._conn.cursor()

        def make_dict(row):
            """Quick function to make dictionary out of row"""
            return dict((key[0], value) for key, value in zip(colnames, row))

        def iter_dates(start, end):
            """Iter date objects for start--end, INCLUSIVE of end date"""
            one_day = timedelta(days=1)
            date = start
            while date <= end:
                yield date
                date += one_day

        weekdays = ['m', 't', 'w', 'th', 'f', 's', 'su']
        # For every row in the calendar...
        for row in cur:
            row = make_dict(row)
            service_I = int(row['service_I'])
            # EXCEPTIONS (calendar_dates): Get a set of all
            # exceptional days.  exception_type=2 means that service
            # removed on that day.  Below, we will exclude all dates
            # that are in this set.
            cur2.execute('SELECT date FROM calendar_dates '
                         'WHERE service_I=? and exception_type=?',
                         (service_I, 2))
            exception_dates = set(x[0] for x in cur2.fetchall())
            #
            start_date = datetime.strptime(row['start_date'], '%Y-%m-%d').date()
            end_date   = datetime.strptime(row['end_date'],   '%Y-%m-%d').date()
            # For every date in that row's date range...
            for date in iter_dates(start_date, end_date):
                weekday = date.isoweekday() - 1  # -1 to match weekdays list above
                # Exclude dates with service exceptions
                date_str = date.strftime('%Y-%m-%d')
                if date_str in exception_dates:
                    #print "calendar_dates.txt exception: removing %s from %s"%(service_I, date)
                    continue
                # If this weekday is marked as true in the calendar...
                if row[weekdays[weekday]]:
                    # Make a list of this service ID being active then.
                    days.append((date, service_I))

        # Store in database, day_start_ut is "noon minus 12 hours".
        cur.executemany("""INSERT INTO days
                       (date, day_start_ut, trip_I)
                       SELECT ?, strftime('%s', ?, '12:00', 'utc')-43200, trip_I
                       FROM trips WHERE service_I=?
                       """, ((date, date, service_I)
                             for date, service_I in days))

        # EXCEPTIONS: Add in dates with exceptions.  Find them and
        # store them directly in the database.
        cur2.execute("INSERT INTO days "
                     "(date, day_start_ut, trip_I) "
                     "SELECT date, strftime('%s',date,'12:00','utc')-43200, trip_I "
                     "FROM trips "
                     "JOIN calendar_dates USING(service_I) "
                     "WHERE exception_type=?",
                     (1, ))

        self._conn.commit()
        # re-create the indexes
        cur.execute('DROP INDEX IF EXISTS main.idx_calendar_dates_sid')

    def index(self, cur):
        cur.execute('CREATE INDEX IF NOT EXISTS idx_days_day ON days (date)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_days_dsut_tid ON days (day_start_ut, trip_I)')


class MetadataLoader(TableLoader):
    """Table to be used for any type of metadata"""
    fname = None
    table = 'metadata'
    tabledef = '(key TEXT UNIQUE NOT NULL, value BLOB, value2 BLOB)'

    @classmethod
    def index(cls, cur):
        cur.execute('CREATE INDEX IF NOT EXISTS idx_metadata_name '
                    'ON metadata (key)')

    @classmethod
    def copy(cls, conn, **where):
        pass


class StopDistancesLoader(TableLoader):
    """Loader to calculate transfer distances.

    The stop_distances table has the following columns:
    from_stop_I:        reference stop_I
    to_stop_I:          reference stop_I
    d:                  straight-line distance (M)
    d_walk:             distance, routed somehow
    min_transfer_time:  from transfers.txt, in seconds.
    """
    # This loader is special.  calc_transfers creates the table there,
    # too.  We put a tabledef here so that copy() will work.
    fname = None
    table = 'stop_distances'
    tabledef = calc_transfers.create_stmt
    threshold = 1000

    def post_import(self, cur):
        # why is cur not used?
        conn = self._conn
        cur = conn.cursor()
        cur2 = conn.cursor()
        if self.print_progress:
            print("Calculating transfers")
        calc_transfers.calc_transfers(conn, threshold=self.threshold)

        # Copy data from transfers table.  Several steps below.
        if self.print_progress:
            print("Copying information from transfers to stop_distances.")
        calc_transfers.bind_functions(conn)

        # Add min transfer times (transfer_type=2).  This just copies
        # min_transfer_time from `transfers` to `stop_distances`.
        stmt = ('SELECT min_transfer_time, from_stop_I, to_stop_I '
                'FROM transfers '
                'WHERE transfer_type=2 '
                'and from_stop_I!=to_stop_I')
        # First we have to run with INSERT OR IGNORE to add in any
        # rows that are missing.  Unfortunately there is no INSERT OR
        # UPDATE, so we do this in two stages.  First we insert any
        # missing rows (there is a unique constraint on (from_stop_I,
        # to_stop_I)) and then we update all rows.
        cur.execute(stmt)
        cur2.executemany('INSERT OR IGNORE INTO stop_distances '
                         '(min_transfer_time, from_stop_I, to_stop_I) '
                         'VALUES (?,?,?)',
                         cur)
        # Now, run again to do UPDATE any pre-existing rows.
        cur.execute(stmt)
        cur2.executemany('UPDATE stop_distances '
                         'SET min_transfer_time=? '
                         'WHERE from_stop_I=? and to_stop_I=?',
                         cur)
        conn.commit()

        # Add timed transfers (transfer_type=1).  This is added with
        # timed_transfer=1 and min_transfer_time=0.  Again, first we
        # add missing rows, and then we update the relevant rows.
        stmt = ('SELECT from_stop_I, to_stop_I '
                'FROM transfers '
                'WHERE transfer_type=1 '
                'and from_stop_I!=to_stop_I')
        cur.execute(stmt)
        cur2.executemany('INSERT OR IGNORE INTO stop_distances '
                         '(from_stop_I, to_stop_I) '
                         'VALUES (?,?)',
                         cur)
        cur.execute(stmt)
        cur2.executemany('UPDATE stop_distances '
                         'SET timed_transfer=1, '
                         '    min_transfer_time=0 '
                         'WHERE from_stop_I=? and to_stop_I=?',
                         cur)
        conn.commit()

        # Excluded transfers.  Delete any transfer point with
        # transfer_type=3.
        cur = conn.cursor()
        cur2 = conn.cursor()
        cur.execute('SELECT from_stop_I, to_stop_I '
                    'FROM transfers '
                    'WHERE transfer_type=3')
        cur2.executemany('DELETE FROM stop_distances '
                         'WHERE from_stop_I=? and to_stop_I=?',
                         cur)
        conn.commit()

        # Calculate any `d`s missing because of inserted rows in the
        # previous two steps.
        cur.execute('UPDATE stop_distances '
                    'SET d=CAST (find_distance('
                    ' (SELECT lat FROM stops WHERE stop_I=from_stop_I), '
                    ' (SELECT lon FROM stops WHERE stop_I=from_stop_I), '
                    ' (SELECT lat FROM stops WHERE stop_I=to_stop_I), '
                    ' (SELECT lon FROM stops WHERE stop_I=to_stop_I)  ) '
                    ' AS INT)'
                    'WHERE d ISNULL'
                    )
        conn.commit()

        # Set all d_walk distances to the same as `d` values.  This
        # would be overwritten in a later step, when routed d_walks
        # are set.
        conn.execute('UPDATE stop_distances SET d_walk=d')
        conn.commit()

    def export_stop_distances(self, conn, f_out):
        cur = conn.cursor()
        cur.execute('SELECT '
                    'from_stop_I, to_stop_I, '
                    'S1.lat, S1.lon, S2.lat, S2.lon, '
                    'd, '
                    'min_transfer_time '
                    'FROM stop_distances '
                    'LEFT JOIN stops S1 ON (from_stop_I=S1.stop_I)'
                    'LEFT JOIN stops S2 ON (to_stop_I  =S2.stop_I)'
                    )
        f_out.write('#from_stop_I,to_stop_I,'
                    'lat1,lon1,lat2,lon2,'
                    'd,min_transfer_time\n')
        for row in cur:
            f_out.write(','.join(str(x) for x in row) + '\n')


def calculate_trip_shape_breakpoints(conn):
    """Pre-compute the shape points corresponding to each trip's stop.

    Depends: shapes"""
    from gtfspy import shapes
    
    cur = conn.cursor()
    breakpoints_cache = {}

    # Counters for problems - don't print every problem.
    count_bad_shape_ordering = 0
    count_bad_shape_fit = 0
    count_no_shape_fit = 0

    trip_Is = [x[0] for x in
               cur.execute('SELECT DISTINCT trip_I FROM stop_times').fetchall()]
    for trip_I in trip_Is:
        # Get the shape points
        row = cur.execute('''SELECT shape_id
                                  FROM trips WHERE trip_I=?''', (trip_I,)).fetchone()
        if row is None:
            continue
        shape_id = row[0]
        if shape_id is None or shape_id == '':
            continue

        # Get the stop points
        cur.execute('''SELECT seq, lat, lon, stop_id
                       FROM stop_times LEFT JOIN stops USING (stop_I)
                       WHERE trip_I=?
                       ORDER BY seq''',
                    (trip_I,))
        #print '%20s, %s'%(run_code, datetime.fromtimestamp(run_sch_starttime))
        stop_points = [dict(seq=row[0],
                            lat=row[1],
                            lon=row[2],
                            stop_I=row[3])
                       for row in cur if row[1] and row[2]]
        # Calculate a cache key for this sequence.  If shape_id and
        # all stop_Is are the same, then we assume that it is the same
        # route and re-use existing breakpoints.
        cache_key = (shape_id, tuple(x['stop_I'] for x in stop_points))
        if cache_key in breakpoints_cache:
            breakpoints = breakpoints_cache[cache_key]

        else:
            # Must re-calculate breakpoints:

            shape_points = shapes.get_shape_points(cur, shape_id)
            breakpoints, badness \
                = shapes.find_segments(stop_points, shape_points)
            if breakpoints != sorted(breakpoints):
                # route_name, route_id, route_I, trip_id, trip_I = \
                #    cur.execute('''SELECT name, route_id, route_I, trip_id, trip_I
                #                 FROM trips LEFT JOIN routes USING (route_I)
                #                 WHERE trip_I=? LIMIT 1''', (trip_I,)).fetchone()
                # print "Ignoring: Route with bad shape ordering:", route_name, route_id, route_I, trip_id, trip_I
                count_bad_shape_ordering += 1
                # select * from stop_times where trip_I=NNNN order by shape_break;
                breakpoints_cache[cache_key] = None
                continue  # Do not set shape_break for this trip.
            # Add it to cache
            breakpoints_cache[cache_key] = breakpoints

            if badness > 30 * len(breakpoints):
                #print "bad shape fit: %s (%s, %s, %s)" % (badness, trip_I, shape_id, len(breakpoints))
                count_bad_shape_fit += 1

        if breakpoints is None:
            continue

        if len(breakpoints) == 0:
            #  No valid route could be identified.
            #print "Ignoring: No shape identified for trip_I=%s, shape_id=%s" % (trip_I, shape_id)
            count_no_shape_fit += 1
            continue

        # breakpoints is the corresponding points for each stop
        assert len(breakpoints) == len(stop_points)
        cur.executemany('UPDATE stop_times SET shape_break=? '
                        'WHERE trip_I=? AND seq=? ',
                        ((bkpt, trip_I, stpt['seq'])
                         for bkpt, stpt in zip(breakpoints, stop_points)))
    if count_bad_shape_fit > 0:
        print(" Shape trip breakpoints: %s bad fits" % count_bad_shape_fit)
    if count_bad_shape_ordering > 0:
        print(" Shape trip breakpoints: %s bad shape orderings" % count_bad_shape_ordering)
    if count_no_shape_fit > 0:
        print(" Shape trip breakpoints: %s no shape fits" % count_no_shape_fit)
    conn.commit()


def validate_day_start_ut(conn):
    """This validates the day_start_ut of the days table."""
    G = GTFS(conn)
    cur = conn.execute('SELECT date, day_start_ut FROM days')
    for date, day_start_ut in cur:
        #print date, day_start_ut
        assert day_start_ut == G.get_day_start_ut(date)


def main_make_views(gtfs_fname):
    """Re-create all views.
    """
    print("creating views")
    conn = GTFS(fname=gtfs_fname).conn
    for L in Loaders:
        L(None).make_views(conn)
    conn.commit()


# OBSOLETE, not supported
# See gtfs.copy_and_filter for similar functionality
#
# def main_copy(source, dest, **where):
#     """Copy one database to another.
#
#     This function is designed to copy one GTFS database to another.  In
#     particular, it would be used to materialize the day_* views.
#
#     Parameters
#     ----------
#     source : sqlite database?
#     dest: sqlite database?
#     """
#     time_import_start = time.time()
#     conn = db.connect_gtfs(None, fname=dest, mode='w')
#     conn.execute('PRAGMA page_size = 4096;')
#     conn.execute('PRAGMA mmap_size = 1073741824;')
#     conn.execute('PRAGMA cache_size = -2000000;')
#     conn.execute('PRAGMA journal_mode = OFF;')
#     #conn.execute('PRAGMA journal_mode = WAL;')
#     conn.execute('PRAGMA synchronous = OFF;')
#
#     conn.execute('ATTACH DATABASE ? AS source', (source.decode('utf-8'),))
#
#     #where = { }
#     G0 = GTFS(source)
#     G0.tzset()
#     if 'start_ut' in where:
#         if not isinstance(where['start_ut'], int):
#             where['start_ut'] = G0.get_day_start_ut(where['start_ut'])
#     if 'end_ut' in where:
#         if not isinstance(where['end_ut'], int):
#             where['end_ut'] = G0.get_day_start_ut(where['end_ut'])
#
#     #where['start_ut'] = G0.get_day_start_ut('2015-08-12')
#     #where['end_ut'] = G0.get_day_start_ut('2015-08-15')
#
#     print "Copying %s -> %s  (%s)" % (source, dest, where)
#     for L in Loaders:
#         print L.__name__
#         L(None).create_table(conn)
#         L(None).create_index(conn)
#         L.copy(conn, **where)
#     for L in Loaders:
#         L(None).make_views(conn)
#     # Recursively detete data
#     print "Recursively deleting data..."
#     conn.execute('DELETE FROM main.trips WHERE '
#                  'trip_I NOT IN (SELECT trip_I FROM main.days)')
#     conn.execute('DELETE FROM main.shapes WHERE '
#                  'shape_id NOT IN (SELECT shape_id FROM main.trips)')
#     conn.execute('DELETE FROM main.stop_times WHERE '
#                  'trip_I NOT IN (SELECT trip_I FROM main.days)')
#     conn.execute('DELETE FROM main.stops WHERE '
#                  'stop_I NOT IN (SELECT stop_I FROM main.stop_times)')
#     conn.execute('DELETE FROM main.stops_rtree WHERE '
#                  'stop_I NOT IN (SELECT stop_I FROM main.stops)')
#     conn.execute('DELETE FROM main.stop_distances WHERE '
#                  '   from_stop_I NOT IN (SELECT stop_I FROM main.stops) '
#                  'OR to_stop_I   NOT IN (SELECT stop_I FROM main.stops)')
#     conn.execute('DELETE FROM main.routes WHERE '
#                  'route_I NOT IN (SELECT route_I FROM main.trips)')
#     conn.execute('DELETE FROM main.agencies WHERE '
#                  'agency_I NOT IN (SELECT agency_I FROM main.routes)')
#     conn.commit()
#
#     # Check that we have some data left
#     count = conn.execute('select count(*) from days').fetchone()
#     if count is None or count[0] == 0:
#         raise ValueError("This DB has no data after copying: %s" % source)
#
#     # Metadata for the copy
#     G = GTFS(dest)
#     G.meta['copied_from'] = source
#     G.meta['copy_time_ut'] = time.time()
#     G.meta['copy_time'] = time.ctime()
#     G.meta['copy_seconds'] = time.time() - time_import_start
#     # Copy some keys directly.
#     for key in ['original_gtfs', 'download_date', 'location_name',
#                 'timezone', ]:
#         G.meta[key] = G0.meta[key]
#     # Update *all* original metadata under orig_ namespace.
#     G.meta.update(('orig_' + k, v) for k, v in G0.meta.items())
#     G.calc_and_store_stats()
#
#     # Must be detached first, or else timestamps will be updated and
#     # make will get messed up.
#     conn.execute('DETACH DATABASE source')
#
#     print "Vacuuming..."
#     conn.execute('VACUUM;')
#     print "Analyzing..."
#     conn.execute('ANALYZE;')
#     del G0, G


Loaders = [AgencyLoader,         # deps: -
           RouteLoader,          # deps: Agency
           MetadataLoader,       # deps: -
           CalendarLoader,       # deps: -
           CalendarDatesLoader,  # deps: Calendar
           ShapeLoader,          # deps: -
           FeedInfoLoader,       # deps: -
           StopLoader,           # deps: -
           StopRtreeLoader,      # deps: Stop
           TransfersLoader,      # deps: Stop
           StopDistancesLoader,  # deps: (pi: Stop)
           TripLoader,           # deps: Route, Calendar, (Shape)             | (pi2: StopTimes)
           StopTimesLoader,      # deps: Stop, Trip                           |                  |(v: Trip, Day)
           FrequenciesLoader,    # deps: Trip (pi: Trip, StopTimes)           |
           DayLoader,            # deps: (pi: Calendar, CalendarDates, Trip)  |
           DayTripsMaterializer  # deps:                                      | (pi2: Day)
           ]
postprocessors = [
    #validate_day_start_ut,
    ]
ignore_tables = set()


def import_gtfs(gtfs_sources, output, preserve_connection=False,
                print_progress=True, location_name=None, **kwargs):
    """Import a GTFS database

    gtfs_sources: str, dict, list
        path to the gtfs zip file or to the dir containing
        or alternatively, a dict mapping gtfs filenames
        (like 'stops.txt' and 'agencies.txt') into their
        strings presentation.
    output: str or sqlite3.Connection
        path to the new database to be created, or an existing
        sqlite3 connection
    preserve_connection: bool, optional
        Whether to close the connection in the end, or not.
    print_progress: bool, optional
        Whether to print progress output
    """

    if isinstance(output, sqlite3.Connection):
        conn = output
    else:
        #if os.path.isfile(output):
         #   raise RuntimeError('File already exists')
        conn = sqlite3.connect(output)
    if not isinstance(gtfs_sources, list):
        gtfs_sources = [gtfs_sources]
    cur = conn.cursor()
    time_import_start = time.time()
    # Use this for quick, fast checks on things.
    #StopLoader(gtfsdir).import_(conn)
    #StopLoader(gtfsdir).post_import(cur)
    #StopRtreeLoader(gtfsdir).import_(conn)
    #StopRtreeLoader(gtfsdir).post_import(cur)
    #import calc_transfers ; calc_transfers.calc_transfers(conn)
    #conn.commit()
    #exit(0)

    # These are a bit unsafe, but make importing much faster,
    # especially on scratch.
    cur.execute('PRAGMA page_size = 4096;')
    cur.execute('PRAGMA mmap_size = 1073741824;')
    cur.execute('PRAGMA cache_size = -2000000;')
    # Changes of isolation level are python3.6 workarounds -
    # eventually will probably be fixed and this can be removed.
    conn.isolation_level = None  # change to autocommit mode (former default)
    cur.execute('PRAGMA journal_mode = OFF;')
    #cur.execute('PRAGMA journal_mode = WAL;')
    cur.execute('PRAGMA synchronous = OFF;')
    conn.isolation_level = ''    # change back to python default.
    # end python3.6 workaround

    #TableLoader.mode = 'index'
    # Do the actual importing.
    loaders = [L(gtfssource=gtfs_sources, print_progress=print_progress, **kwargs) for L in Loaders]

    # Do initial import.  This consists of making tables, raw insert
    # of the CSVs, and then indexing.
    for Loader in loaders:
        Loader.import_(conn)

    # Do any operations that require all tables present.
    for Loader in loaders:
        Loader.post_import_round2(conn)

    # Make any views
    for Loader in loaders:
        Loader.make_views(conn)

    # Make any views
    for F in postprocessors:
        F(conn)

    # Set up same basic metadata.
    from gtfspy import gtfs as mod_gtfs
    G = mod_gtfs.GTFS(output)
    G.meta['gen_time_ut'] = time.time()
    G.meta['gen_time'] = time.ctime()
    G.meta['import_seconds'] = time.time() - time_import_start
    G.meta['download_date'] = ''
    G.meta['location_name'] = ''
    G.meta['n_gtfs_sources'] = len(gtfs_sources)
    # Extract things from GTFS
    for i, source in enumerate(gtfs_sources):
        if len(gtfs_sources) == 1:
            prefix = ""
        else:
            prefix = "feed_" + str(i) + "_"
        if isinstance(source, string_types):
            G.meta[prefix + 'original_gtfs'] = decode_six(source) if source else None
            # Extract GTFS date.  Last date pattern in filename.
            filename_date_list = re.findall(r'\d{4}-\d{2}-\d{2}', source)
            if filename_date_list:
                G.meta[prefix + 'download_date'] = filename_date_list[-1]
            if location_name:
                G.meta['location_name'] = location_name
            else:
                location_name_list = re.findall(r'/([^/]+)/\d{4}-\d{2}-\d{2}', source)
                if location_name_list:
                    G.meta[prefix + 'location_name'] = location_name_list[-1]
                else:
                    try:
                        G.meta[prefix + 'location_name'] = source.split("/")[-4]
                    except:
                        G.meta[prefix + 'location_name'] = source

    G.meta['timezone'] = cur.execute('SELECT timezone FROM agencies LIMIT 1').fetchone()[0]
    stats.update_stats(G)
    del G

    if print_progress:
        print("Vacuuming...")
    # Next 3 lines are python 3.6 work-arounds again.
    conn.isolation_level = None  # former default of autocommit mode
    cur.execute('VACUUM;')
    conn.isolation_level = ''    # back to python default
    # end python3.6 workaround
    if print_progress:
        print("Analyzing...")
    cur.execute('ANALYZE')
    if not (preserve_connection is True):
        conn.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="""
    Import GTFS files.  Imports gtfs.  There are two subcommands.  The
    'import' subcommand converts from a GTFS directory or zipfile to a
    sqlite database.  Both must be specified on the command line.  The
    import-auto subcommand is older, and uses the logic in code/db.py to
    automatically find databases and output files (in scratch/gtfs and
    scratch/db) based on the shortname given on the command line.  This
    should probably not be used much anymore, instead automate that before
    calling this program.""")
    subparsers = parser.add_subparsers(dest='cmd')
    # parsing import
    parser_import = subparsers.add_parser('import', help="Direct import GTFS->sqlite")
    parser_import.add_argument('gtfs', help='Input GTFS filename (dir or .zip)')
    parser_import.add_argument('output', help='Output .sqlite filename (must end in .sqlite)')
    parser.add_argument('--fast', action='store_true',
                        help='Skip stop_times and shapes tables.')

    # parsing import-auto
    parser_importauto = subparsers.add_parser('import-auto', help="Automatic GTFS import from files")
    parser_importauto.add_argument('gtfsname', help='Input GTFS filename')

    # parsing import-multiple
    parser_import_multiple = subparsers.add_parser('import-multiple', help="GTFS import from multiple zip-files")
    parser_import_multiple.add_argument('zipfiles', metavar='zipfiles', type=str, nargs=argparse.ONE_OR_MORE,
                                        help='zipfiles for the import')
    parser_import_multiple.add_argument('output', help='Output .sqlite filename (must end in .sqlite)')

    # parsing import-list

    # Parsing copy
    parser_copy = subparsers.add_parser('copy', help="Copy database")
    parser_copy.add_argument('source', help='Input GTFS .sqlite')
    parser_copy.add_argument('dest', help='Output GTFS .sqlite')
    parser_copy.add_argument('--start', help='Start copy time (inclusive)')
    parser_copy.add_argument('--end', help='Start copy time (exclusive)')

    # Parsing copy
    parser_copy = subparsers.add_parser('make-views', help="Re-create views")
    parser_copy.add_argument('gtfs', help='Input GTFS .sqlite')

    # make-weekly-download
    parser_copy = subparsers.add_parser('make-weekly')
    parser_copy.add_argument('source', help='Input GTFS .sqlite')
    parser_copy.add_argument('dest', help='Output GTFS .sqlite')

    parser_copy = subparsers.add_parser('make-daily')
    parser_copy.add_argument('source', help='Input GTFS .sqlite')
    parser_copy.add_argument('dest', help='Output GTFS .sqlite')

    # Export stop distances
    parser_copy = subparsers.add_parser('export-stop-distances')
    parser_copy.add_argument('gtfs', help='Input GTFS .sqlite')
    parser_copy.add_argument('output', help='Output for .txt file')

    # Custom stuff
    parser_copy = subparsers.add_parser('custom')
    parser_copy.add_argument('gtfs', help='Input GTFS .sqlite')

    args = parser.parse_args()
    if args.fast:
        ignore_tables.update(('stop_times', 'shapes'))

    # if the first argument is import, import a GTFS directory to a .sqlite database.
    # Both directory and
    if args.cmd == 'import':
        gtfs = args.gtfs
        output = args.output
        # This context manager makes a tmpfile for import.  If there
        # is corruption during import, it won't leave a incomplete or
        # corrupt file where it will be noticed.
        with util.create_file(output, tmpdir=True, keepext=True) as tmpfile:
            import_gtfs(gtfs, output=tmpfile)
    elif args.cmd == "import-multiple":
        zipfiles = args.zipfiles
        output = args.output
        with util.create_file(output, tmpdir=True, keepext=True) as tmpfile:
            import_gtfs(zipfiles, output=tmpfile)
    elif args.cmd == 'make-views':
        main_make_views(args.gtfs)
    # This is now implemented in gtfs.py, please remove the commented code
    # if no one has touched this in a while.:
    #
    # elif args.cmd == 'make-weekly':
    #     G = GTFS(args.source)
    #     download_date = G.meta['download_date']
    #     d = datetime.strptime(download_date, '%Y-%m-%d').date()
    #     date_start = d + timedelta(7-d.isoweekday()+1)      # inclusive
    #     date_end   = d + timedelta(7-d.isoweekday()+1 + 7)  # exclusive
    #     G.copy_and_filter(args.dest, start_date=date_start, end_date=date_end)
    # elif args.cmd == 'make-daily':
    #     G = GTFS(args.source)
    #     download_date = G.meta['download_date']
    #     d = datetime.strptime(download_date, '%Y-%m-%d').date()
    #     date_start = d + timedelta(7-d.isoweekday()+1)      # inclusive
    #     date_end   = d + timedelta(7-d.isoweekday()+1 + 1)  # exclusive
    #     G.copy_and_filter(args.dest, start_date=date_start, end_date=date_end)
    elif args.cmd == 'export-stop-distances':
        conn = sqlite3.connect(args.gtfs)
        L = StopDistancesLoader(conn)
        L.export_stop_distances(conn, open(args.output, 'w'))
    elif args.cmd == 'custom':
        pass
        # This is designed for just testing things.  This code should
        # always be commented out in the VCS.
        # conn = sqlite3.connect(args.gtfs)
        # L = StopDistancesLoader(conn)
        # L.post_import(None)
        # L.export_stop_distances(conn, open('sd.txt', 'w'))
    else:
        print("Unrecognized command: %s" % args.cmd)
        exit(1)

if __name__ == "__main__":
    main()
