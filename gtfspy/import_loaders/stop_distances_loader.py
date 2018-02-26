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

    def post_import(self, conn):
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
