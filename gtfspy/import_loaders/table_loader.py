import codecs
import csv
import os
import sys
import zipfile

from six import string_types

from gtfspy import util


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
        gtfssource: str, dict, list
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
            list: a list of the above elements to import (i.e. "merge") multiple GTFS feeds to the same database

        print_progress: boolean
            whether to print progress of the
        """
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
        return any(self.exists_by_source())

    def exists_by_source(self):
        """Does this GTFS contain this file? (file specified by the class)"""
        exists_list = []
        for source in self.gtfs_sources:
            if isinstance(source, dict):
                # source can now be either a dict or a zipfile
                if self.fname in source:
                    if source[self.fname]:
                        exists_list.append(True)
                        continue
            # Handle zipfiles specially
            if "zipfile" in source:
                try:
                    Z = zipfile.ZipFile(source['zipfile'], mode='r')
                    Z.getinfo(os.path.join(source['zip_commonprefix'], self.fname))
                    exists_list.append(True)
                    continue
                # File does not exist in the zip archive
                except KeyError:
                    print(self.fname, ' missing in ', source)
                    exists_list.append(False)
                    continue
            # Normal filename
            elif isinstance(source, string_types):
                if os.path.exists(os.path.join(source, self.fname)):
                    exists_list.append(True)
                    continue
            exists_list.append(False)
        # the "file" was not found in any of the sources, return false
        return exists_list

    def assert_exists_if_required(self):
        REQUIRED_FILES_GTFS = ["agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]
        if self.fname in REQUIRED_FILES_GTFS:
            for gtfs_source, exists in zip(self.gtfs_sources, self.exists_by_source()):
                if not exists:
                    raise AssertionError(self.fname + " does not exist in the provided gtfs_source")

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
            # This hack removes the BOM from the start of any
            # line.
            for line in file_obj:
                yield line.lstrip(codecs.BOM_UTF8.decode("utf-8"))

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


ignore_tables = set()


def decode_six(string):
    version = sys.version_info[0]
    if version == 2:
        return string.decode('utf-8')
    else:
        assert(isinstance(string, str))
        return string