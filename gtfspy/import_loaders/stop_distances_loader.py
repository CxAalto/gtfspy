from gtfspy import calc_transfers
from gtfspy.import_loaders.table_loader import TableLoader


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
            print("Calculating straight-line transfer distances")
        calc_transfers.calc_transfers(conn, threshold_meters=self.threshold)

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