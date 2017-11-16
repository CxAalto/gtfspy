from gtfspy.import_loaders.table_loader import TableLoader, decode_six


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

    def post_import_round2(self, conn):
        update_trip_travel_times_ds(conn)

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


def update_trip_travel_times_ds(conn):
    cur0 = conn.cursor()
    cur = conn.cursor()
    cur0.execute('''SELECT trip_I, min(dep_time), max(arr_time)
                   FROM trips JOIN stop_times USING (trip_I)
                   GROUP BY trip_I''')

    print("updating trips travel times")

    def iter_rows(cur0):
        for row in cur0:
            if row[1]:
                st = row[1].split(':')
                start_time_ds = int(st[0]) * 3600 + int(st[1]) * 60 + int(st[2])
            else:
                start_time_ds = None
            if row[2]:
                et = row[2].split(':')
                end_time_ds = int(et[0]) * 3600 + int(et[1]) * 60 + int(et[2])
            else:
                end_time_ds = None
            yield start_time_ds, end_time_ds, row[0]

    cur.executemany('''UPDATE trips SET start_time_ds=?, end_time_ds=? WHERE trip_I=?''',
                    iter_rows(cur0))
    conn.commit()
