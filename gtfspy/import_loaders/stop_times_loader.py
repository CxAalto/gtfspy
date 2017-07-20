from gtfspy.import_gtfs import decode_six
from gtfspy.import_loaders.table_loader import TableLoader
from gtfspy.import_loaders.stop_distances_loader import calculate_trip_shape_breakpoints


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