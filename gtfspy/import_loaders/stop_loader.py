from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class StopLoader(TableLoader):
    # This class is documented to explain what it does, others are not.
    # Metadata needed to create table.  GTFS filename, table name, and
    # the CREATE TABLE syntax (last part only).
    fname = 'stops.txt'
    table = 'stops'
    tabledef = '''(stop_I INTEGER PRIMARY KEY, stop_id TEXT UNIQUE NOT NULL, code TEXT, name TEXT, desc TEXT, lat REAL, lon REAL, parent_I INT, location_type INT, wheelchair_boarding BOOL, self_or_parent_I INT)'''

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

    def index(self, cur):
        # Make indexes/ views as needed.
        #cur.execute('CREATE INDEX IF NOT EXISTS idx_stop_sid ON stop (stop_id)')
        #cur.execute('CREATE INDEX IF NOT EXISTS idx_stops_pid_sid ON stops (parent_id, stop_I)')
        #conn.commit()
        pass