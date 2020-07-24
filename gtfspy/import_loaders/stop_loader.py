from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class StopLoader(TableLoader):
    # This class is documented to explain what it does, others are not.
    # Metadata needed to create table.  GTFS filename, table name, and
    # the CREATE TABLE syntax (last part only).
    fname = 'stops.txt'
    table = 'stops'
    tabledef = '''(stop_I INTEGER PRIMARY KEY, stop_id TEXT UNIQUE NOT NULL, code TEXT, name TEXT, desc TEXT, lat REAL, 
    lon REAL, parent_I INT, location_type INT, wheelchair_boarding BOOL, self_or_parent_I INT)'''

    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                # and transform the "row" dictionary into a new
                # dictionary, which is yielded.  There can be different
                # transformations here, as needed.
                yield dict(
                    stop_id       = prefix + decode_six(row['stop_id']),
                    code          = decode_six(row['stop_code']) if 'stop_code' in row else None,
                    name          = decode_six(row['stop_name']),
                    desc          = decode_six(row['stop_desc']) if 'stop_desc' in row else None,
                    lat           = float(row['stop_lat']),
                    lon           = float(row['stop_lon']),
                    _parent_id    = prefix + decode_six(row['parent_station']) if row.get('parent_station', '') and
                                                                                  decode_six(row['stop_id']) !=
                                                                                  decode_six(row['parent_station']) else None,
                    location_type = int(row['location_type']) if row.get('location_type') else None,
                    wheelchair_boarding = int(row['wheelchair_boarding']) if row.get('wheelchair_boarding', '') else None,
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
        cur.execute("SELECT InitSpatialMetaData()")
        cur.execute("SELECT AddGeometryColumn ('stops', 'geometry', 4326, 'POINT', 2)")
        cur.execute("""UPDATE stops SET geometry=MakePoint(lon, lat, 4326)""")
        cur.execute("SELECT CreateSpatialIndex('stops', 'geometry');")

    def index(self, cur):
        # Make indexes/ views as needed.
        cur.execute('CREATE INDEX IF NOT EXISTS idx_stop_sid ON stops (stop_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_stops_pid_sid ON stops (parent_I, stop_I)')
        #cur.commit()
        #pass

    def post_import_round2(self, cur):
        q1 = """CREATE TABLE stop_intervals (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                from_stop_I INTEGER,
                to_stop_I INTEGER,
                type INTEGER,
                freq INTEGER);"""

        q2 = """SELECT AddGeometryColumn('stop_intervals', 'geometry', 4326, 'LINESTRING', 'XY');"""
        q3 = """INSERT INTO stop_intervals (geometry, from_stop_I, to_stop_I, type, freq)
        WITH 
        stimes AS (SELECT * FROM stop_times, trips, routes 
                   WHERE stop_times.trip_I = trips.trip_I 
                   AND trips.route_I = routes.route_I),
        s AS (SELECT * FROM stops)
        SELECT 
        MakeLine(s1.geometry, s2.geometry) AS geometry, 
        s1.stop_I AS from_stop_I,
        s2.stop_I AS to_stop_I,
        q1.type,
        CAST(COUNT(*) AS INTEGER) AS freq 
        FROM
        (stimes) q1,
        (stimes) q2,
        (s) s1,
        (s) s2
        WHERE q1.seq+1=q2.seq AND q1.trip_I=q2.trip_I AND s1.stop_I = q1.stop_I AND s2.stop_I = q2.stop_I
        GROUP BY q1.stop_I, q2.stop_I"""

        q4 = "SELECT CreateSpatialIndex('stop_intervals', 'geometry');"
        for q in [q1, q2, q3, q4]:
            cur.execute(q)
