import math
import sqlite3

from gtfspy.util import wgs84_distance, wgs84_height, wgs84_width
from gtfspy.gtfs import GTFS

create_stmt = ('CREATE TABLE IF NOT EXISTS main.stop_distances '
               '(from_stop_I INT, '
               ' to_stop_I INT, '
               ' d INT, '
               ' d_walk INT, '
               ' min_transfer_time INT, '
               ' timed_transfer INT, '
               'UNIQUE (from_stop_I, to_stop_I)'
               ')'
               )


def bind_functions(conn):
    conn.create_function("find_distance", 4, wgs84_distance)
    conn.create_function("wgs84_height", 1, wgs84_height)
    conn.create_function("wgs84_width", 2, wgs84_width)


def calc_transfers(conn, threshold=1000):
    """Add transfer information to a GTFS database.

    This adds a table 'stop_distances' to the database.  All stops
    closer than 1000m are included as a pair here, with a column for
    their distance.

    Parameters
    ----------
    conn :
        sqlite3.Connection
    threshold : int
        Distance threshold in meters
    """
    bind_functions(conn)
    cur = conn.cursor()
    cur.execute('DROP INDEX IF EXISTS main.idx_sd_fsid')
    cur.execute('DROP TABLE IF EXISTS main.stop_distances')

    cur.execute(create_stmt)
    #cur.execute('INSERT INTO stop_distances '
    #            'SELECT stops1.stop_I as from_stop_I, stops2.stop_I as to_stop_I, '
    #                'CAST(find_distance(stops1.lat,stops1.lon, stops2.lat, stops2.lon) AS INT) AS d '
    #              'FROM stops AS stops1 JOIN stops AS stops2 '
    #              'WHERE (from_stop_I!=to_stop_I) and (d < ?);', (threshold, ))
    #cur.execute(#'INSERT INTO stop_distances2 (from_stop_I, to_stop_I, d)'
    #            'explain query plan '
    #            'SELECT S1.stop_I AS from_stop_I, RT.stop_I AS to_stop_I, '
    #                'CAST(find_distance(S1.lat,S1.lon, S2.lat, S2.lon) AS INT) AS d '
    #              'FROM stops AS S1 '
    #                  'left JOIN stops_rtree AS RT '
    #                  'LEFT JOIN stops AS S2 ON (RT.stop_I=S2.stop_I)'
    #              'WHERE '
    #                  '(from_stop_I != to_stop_I) '
    #                  'AND S1.lat-wgs84_height(?)*1.2 <= RT.lat2  AND RT.lat <= S1.lat+wgs84_height(?)*1.2 '
    #                  'AND S1.lon-wgs84_width(?,S1.lat)*1.2 <= RT.lon2 AND RT.lon <= S1.lon+wgs84_width(?,S1.lat)*1. ',
    #                  #'and (d < ?)',
    #              #'LIMIT 7',
    #                  #(threshold, ),
    #                  (threshold, threshold, threshold, threshold, ),
    #                  #(threshold, threshold, threshold, threshold, threshold, ),
    #                  )
    #cur.execute(#'INSERT INTO stop_distances2 (from_stop_I, to_stop_I, d)'
    #            'explain query plan '
    #            #'SELECT from_stop_I, to_stop_I '
    #            #    'CAST(find_distance(lat1,lon1, S2.lat, S2.lon) AS INT) AS d '
    #            'SELECT * '
    #            'FROM ( '
    #              'SELECT S1.stop_I AS from_stop_I, RT.stop_I AS to_stop_I '
    #            #         'S1.lat AS lat1, S1.lon AS lon1 '
    #              'FROM stops AS S1 '
    #                  'left JOIN stops_rtree AS RT '
    #              'WHERE '
    #                  'S1.lat-wgs84_height(?)*1.2 <= RT.lat2  AND RT.lat <= S1.lat+wgs84_height(?)*1.2 '
    #                  'AND S1.lon-wgs84_width(?,S1.lat)*1.2 <= RT.lon2 AND RT.lon <= S1.lon+wgs84_width(?,S1.lat)*1.2 '
    #                  'AND (from_stop_I != to_stop_I) '
    #            ') SQ'
    #            ', stops AS S2 ',#ON (to_stop_I=S2.stop_I)',
    #            #'WHERE (SQ.to_stop_I=S2.stop_I)',
    #            #'WHERE (d < ?)',
    #              #'LIMIT 7',
    #                  #(threshold, ),
    #                  (threshold, threshold, threshold, threshold, ),
    #                  #(threshold, threshold, threshold, threshold, threshold, ),
    #                  )
    #cur.execute(#'INSERT INTO stop_distances2 (from_stop_I, to_stop_I, d)'
    #            #'explain query plan '
    #            #'SELECT from_stop_I, to_stop_I '
    #            #    'CAST(find_distance(lat1,lon1, S2.lat, S2.lon) AS INT) AS d '
    #
    #            '(SELECT '
    #                'S1.stop_I                   AS from_stop_I, '
    #                'S1.lat+wgs84_height(?)*1.1  AS top, '
    #                'S1.lat-wgs84_height(?)*1.1  AS bottom, '
    #                'S1.lon+wgs84_width(?,S1.lat)*1.1 AS right, '
    #                'S1.lon-wgs84_width(?,S1.lat)*1.1 AS left, '
    #              'FROM stops AS S1) '
    #             'LEFT JOIN stops_rtree AS RT'
    #                 'WHERE '
    #                 '    bottom <= RT.lat2 AND RT.lat <= top '
    #                 'AND left <= RT.lon2   AND RT.lon <= left '
    #
    #
    #
    #            'SELECT * '
    #            'FROM ( '
    #              'SELECT S1.stop_I AS from_stop_I, RT.stop_I AS to_stop_I '
    #            #         'S1.lat AS lat1, S1.lon AS lon1 '
    #              'FROM stops AS S1 '
    #                  'left JOIN stops_rtree AS RT '
    #              'WHERE '
    #                  'unlikely( '
    #                  'S1.lat-wgs84_height(?)*1.2 <= RT.lat2  AND RT.lat <= S1.lat+wgs84_height(?)*1.2 '
    #                  'AND S1.lon-wgs84_width(?,S1.lat)*1.2 <= RT.lon2 AND RT.lon <= S1.lon+wgs84_width(?,S1.lat)*1.2 ) '
    #                  'AND (from_stop_I != to_stop_I) '
    #
    #            ') '
    #            'CROSS JOIN stops AS S2 ON (to_stop_I=S2.stop_I)',
    #            #'WHERE (d < ?)',
    #              #'LIMIT 7',
    #                  #(threshold, ),
    #                  (threshold, threshold, threshold, threshold, ),
    #                  #(threshold, threshold, threshold, threshold, threshold, ),
    #                  )
    cur.execute(#'INSERT INTO stop_distances2 (from_stop_I, to_stop_I, d)'
                #'explain query plan '
                #'SELECT from_stop_I, to_stop_I '
                #    'CAST(find_distance(lat1,lon1, S2.lat, S2.lon) AS INT) AS d '
                'CREATE TEMPORARY TABLE stop_distances_tmp AS '
                'SELECT S1.stop_I AS from_stop_I, RT.stop_I AS to_stop_I, '
                         'S1.lat AS lat1, S1.lon AS lon1 '
                  'FROM stops AS S1 '
                      'JOIN stops_rtree AS RT '
                  'WHERE '
                      'S1.lat-wgs84_height(?)*1.2 <= RT.lat2  AND RT.lat <= S1.lat+wgs84_height(?)*1.2 '
                      'AND S1.lon-wgs84_width(?,S1.lat)*1.2 <= RT.lon2 AND RT.lon <= S1.lon+wgs84_width(?,S1.lat)*1.2 '
                      'AND (from_stop_I != to_stop_I) ',
                #'WHERE (SQ.to_stop_I=S2.stop_I)',
                #'WHERE (d < ?)',
                  #'LIMIT 7',
                      #(threshold, ),
                      (threshold, threshold, threshold, threshold, ),
                      #(threshold, threshold, threshold, threshold, threshold, ),
                      )
    for row in cur:
        print row
    cur.execute('INSERT INTO stop_distances (from_stop_I, to_stop_I, d)'
                #'explain query plan '
                'SELECT from_stop_I, to_stop_I, '
                    'CAST(find_distance(lat1,lon1, S2.lat, S2.lon) AS INT) AS d '
                'FROM stop_distances_tmp S1 '
                    'JOIN stops S2 ON (S1.to_stop_I=S2.stop_I) '
                'WHERE (d < ?)',
                  #'LIMIT 7',
                      (threshold, ),
                      #(threshold, threshold, threshold, threshold, ),
                      #(threshold, threshold, threshold, threshold, threshold, ),
                      )
    for row in cur:
        print row

    cur.execute('CREATE INDEX IF NOT EXISTS idx_sd_fsid ON stop_distances (from_stop_I);')
    cur.execute('DROP TABLE stop_distances_tmp')
    conn.commit()


def export_transfers(conn, fname):
    conn = GTFS(conn).conn
    cur = conn.cursor()
    cur.execute('SELECT S1.lat, S1.lon, S2.lat, S2.lon, SD.d '
                'FROM stop_distances SD '
                '  LEFT JOIN stops S1 ON (SD.from_stop_I=S1.stop_I) '
                '  LEFT JOIN stops S2 ON (SD.to_stop_I  =S2.stop_I)')
    f = open(fname, 'w')
    for row in cur:
        print >> f, ' '.join(str(x) for x in row)


def main():
    import sys
    cmd = sys.argv[1]
    if cmd == 'calc':
        dbname = sys.argv[2]
        conn = GTFS(dbname).conn
        calc_transfers(conn)
    elif cmd == 'export':
        export_transfers(sys.argv[2], sys.argv[3])


if __name__ == "__main__":
    main()


