from gtfspy.import_loaders.table_loader import TableLoader, decode_six


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
        # Calculate a cache key for this sequence.
        # If both shape_id, and all stop_Is are same, then we can re-use existing breakpoints:
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
                        ((int(bkpt), int(trip_I), int(stpt['seq']))
                         for bkpt, stpt in zip(breakpoints, stop_points)))
    if count_bad_shape_fit > 0:
        print(" Shape trip breakpoints: %s bad fits" % count_bad_shape_fit)
    if count_bad_shape_ordering > 0:
        print(" Shape trip breakpoints: %s bad shape orderings" % count_bad_shape_ordering)
    if count_no_shape_fit > 0:
        print(" Shape trip breakpoints: %s no shape fits" % count_no_shape_fit)
    conn.commit()