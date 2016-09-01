"""Shape tools

This module contains tools for dealing with shapes.

The core data structure for representing shapes is
[ {'seq':N, 'lat':F, 'lon':F}, ... ].
`seq` is the integer index of sequence points (note that GTFS doesn't
require integers), and lat/lon are latitude/longitude.  Note that
there is no stop_id or other data stored, though any other data can be
put in the dictionary and it will be preserved.  The 'd' key, if
present, represents cumulative distance (m) along the points.

This same structure is used for representing sequences of stops.  The
function `find_segments` take a shape-sequence and stop-sequence, and
returns a list of `break_points`, which are the indexes within
shape-sequence which correspond to each stop within stop-sequence.

You can then pass `break_points` and the shape-sequence to the
function `return_segments`, which returns a list
[ [ shape-sequence ],
  [ shape-sequence ],
  ...,
  [ ]
]
This is the shape, broken down into individual segments, one
corresponding to each stop.  The last one is empty.


"""
from __future__ import absolute_import

import numpy as np
from .util import wgs84_distance


def print_coords(rows, prefix=''):
    """Print coordinates within a sequence.

    This is only used for debugging.  Printed in a form that can be
    pasted into Python for visualization."""
    lat = [row['lat'] for row in rows]
    lon = [row['lon'] for row in rows]
    print 'COORDS'+'-'*5
    print "%slat, %slon = %r, %r" % (prefix, prefix, lat, lon)
    print '-'*5


def find_segments(stops, shape):
    """Find correspodning shape points for a list of stops and create shape break points.

    Parameters
    ----------
    stops: stop-sequence (list)
        List of stop points
    shape: shape-sequence (list)
        List of shape points

    Returns
    -------
    tuple of:

    break_points: list of ints
        stops[i] corresponds to shape[break_points[i]].  This list can
        be used to partition the shape points into segments between
        one stop and the next.
    badness: float
        Lower indicates better fit to the shape.  This is the sum of
        distances (in meters) between every each stop and its closest
        shape point.  This is not needed in normal use, but in the
        cases where you must determine the best-fitting shape for a
        stop-sequence, use this.
    """
    if not shape:
        return [], 0
    break_points = []
    last_i = 0
    cumul_d = 0
    badness = 0
    d_last_stop = float('inf')
    lstlat, lstlon = None, None
    break_shape_points = []
    for stop in stops:
        stlat, stlon = stop['lat'], stop['lon']
        best_d = float('inf')
        # print stop
        if badness > 500 and badness > 30 * len(break_points):
            return [], badness
        for i in xrange(last_i, len(shape)):
            d = wgs84_distance(stlat, stlon, shape[i]['lat'], shape[i]['lon'])
            if lstlat:
                d_last_stop = wgs84_distance(lstlat, lstlon, shape[i]['lat'], shape[i]['lon'])
            # If we are getting closer to next stop, record this as
            # the best stop so far.continue
            if d < best_d:
                best_d = d
                best_i = i
                # print best_d, i, last_i, len(shape)
                cumul_d += d
            # We have to be very careful about our stop condition.
            # This is trial and error, basically.
            if (d_last_stop < d) or (d > 500) or (i < best_i + 100):
                    continue
            # We have decided our best stop, stop looking and continue
            # the outer loop.
            else:
                badness += best_d
                break_points.append(best_i)
                last_i = best_i
                lstlat, lstlon = stlat, stlon
                break_shape_points.append(shape[best_i])
                break
        else:
            # Executed if we did *not* break the inner loop
            badness += best_d
            break_points.append(best_i)
            last_i = best_i
            lstlat, lstlon = stlat, stlon
            break_shape_points.append(shape[best_i])
            pass
    # print "Badness:", badness
    # print_coords(stops, 'stop')
    # print_coords(shape, 'shape')
    # print_coords(break_shape_points, 'break')
    return break_points, badness


def find_best_segments(cur, stops, shape_ids, route_id=None,
                       breakpoints_cache=None):
    """Finds the best shape_id for a stop-sequence.

    This is used in cases like when you have GPS data with a route
    name, but you don't know the route direction.  It tries shapes
    going both directions and returns the shape that best matches.
    Could be used in other cases as well.

    Parameters
    ----------
    cur : sqlite3.Cursor
        database cursor
    stops : list
    shape_ids : list of shape_id:s
    route_id : route_id to search for stops
    breakpoints_cache : dict
        If given, use this to cache results from this function.
    """
    cache_key = None
    if breakpoints_cache is not None:
        # Calculate a cache key for this sequence.  If shape_id and
        # all stop_Is are the same, then we assume that it is the same
        # route and re-use existing breakpoints.
        cache_key = (route_id, tuple(x['stop_I'] for x in stops))
        if cache_key in breakpoints_cache:
            print 'found in cache'
            return breakpoints_cache[cache_key]

    if route_id is not None:
        cur.execute('''SELECT DISTINCT shape_id
                        FROM routes
                        LEFT JOIN trips
                        USING (route_I)
                        WHERE route_id=?''',
                    (route_id,))
        data = cur.fetchall()
        # If not data, then route_id didn't match anything, or there
        # were no shapes defined.  We have to exit in this case.
        if not data:
            print "No data for route_id=%s" % route_id
            return [], None, None, None
        #
        shape_ids = zip(*data)[0]
    # print 'find_best_segments:', shape_ids
    results = []
    for shape_id in shape_ids:
        shape = get_shape_points(cur, shape_id)
        breakpoints, badness = find_segments(stops, shape)
        results.append([badness, breakpoints, shape, shape_id])
        if len(stops) > 5 and badness < 5*(len(stops)):
            break

    best = np.argmin(zip(*results)[0])
    # print 'best', best
    badness = results[best][0]
    breakpoints = results[best][1]
    shape = results[best][2]
    shape_id = results[best][3]
    if breakpoints_cache is not None:
        print "storing in cache", cache_key[0], hash(cache_key[1:])
        breakpoints_cache[cache_key] = breakpoints, badness, shape, shape_id
    return breakpoints, badness, shape, shape_id


def return_segments(shape, break_points):
    """Break a shape into segments between stops using break_points.

    This function can use the `break_points` outputs from
    `find_segments`, and cuts the shape-sequence into pieces
    corresponding to each stop.
    """
    # print 'xxx'
    # print stops
    # print shape
    # print break_points
    # assert len(stops) == len(break_points)
    segs = []
    bp = 0 # not used
    bp2 = 0
    for i in range(len(break_points)-1):
        bp = break_points[i] if break_points[i] is not None else bp2
        bp2 = break_points[i+1] if break_points[i+1] is not None else bp
        segs.append(shape[bp:bp2+1])
    segs.append([])
    return segs


def gen_cumulative_distances(stops):
    """
    Add a 'd' key for distances to a stop/shape-sequence.

    This takes a shape-sequence or stop-sequence, and adds an extra
    'd' key that is cumulative, geographic distances between each
    point. This uses `wgs84_distance` from the util module.  The
    distances are in meters.  Distances are rounded to the nearest
    integer, because otherwise JSON size increases greatly.

    Parameters
    ----------
    stops: list
        elements are dicts with 'lat' and 'lon' keys
        and the function adds the 'd' key ('d' stands for distance)
        to the dictionaries
    """
    stops[0]['d'] = 0.0
    for i in range(1, len(stops)):
        stops[i]['d'] = stops[i-1]['d'] + wgs84_distance(
            stops[i-1]['lat'], stops[i-1]['lon'],
            stops[i]['lat'], stops[i]['lon'],
            )
    for stop in stops:
        stop['d'] = int(stop['d'])
        # stop['d'] = round(stop['d'], 1)


def get_shape_points(cur, shape_id):
    """
    Given a shape_id, return its shape-sequence.

    Parameters
    ----------
    cur: sqlite3.Cursor
        cursor to a GTFS database
    shape_id: str
        id of the route

    Returns
    -------
    shape_points: list
        elements are dictionaries containing the 'seq', 'lat', and 'lon' of the shape
    """
    cur.execute('''SELECT seq, lat, lon, d FROM shapes where shape_id=?
                    ORDER BY seq''', (shape_id,))
    shape_points = [dict(seq=row[0], lat=row[1], lon=row[2], d=row[3])
                    for row in cur]
    return shape_points


def get_shape_points2(cur, shape_id):
    """
    Given a shape_id, return its shape-sequence (as a dict of lists).
    get_shape_points function returns them as a list of dicts

    Parameters
    ----------
    cur: sqlite3.Cursor
        cursor to a GTFS database
    shape_id: str
        id of the route

    Returns
    -------
    shape_points: dict of lists
        dict contains keys 'seq', 'lat', 'lon', and 'd'(istance) of the shape
    """
    cur.execute('''SELECT seq, lat, lon, d FROM shapes where shape_id=?
                    ORDER BY seq''', (shape_id,))
    shape_points = {'seqs': [], 'lats':  [], 'lons': [], 'd': []}
    for row in cur:
        shape_points['seqs'].append(row[0])
        shape_points['lats'].append(row[1])
        shape_points['lons'].append(row[2])
        shape_points['d'].append(row[3])
    return shape_points


def get_route_shape_segments(cur, route_id):
    """
    Given a route_id, return its stop-sequence.

    Parameters
    ----------
    cur: sqlite3.Cursor
        cursor to a GTFS database
    route_id: str
        id of the route

    Returns
    -------
    shape_points: list
        elements are dictionaries containing the 'seq', 'lat', and 'lon' of the shape
    """
    cur.execute('''SELECT seq, lat, lon
                    FROM (
                        SELECT shape_id
                        FROM route
                        LEFT JOIN trips
                        USING (route_I)
                        WHERE route_id=? limit 1
                        )
                    JOIN shapes
                    USING (shape_id)
                    ORDER BY seq''', (route_id,))
    shape_points = [dict(seq=row[0], lat=row[1], lon=row[2]) for row in cur]
    return shape_points


def get_shape_between_stops(cur, trip_I, seq_stop1=None, seq_stop2=None, shape_breaks=None):
    """
    Given a trip_I (shortened id), return shape points between two stops
    (seq_stop1 and seq_stop2).

    Trip_I is used for matching obtaining the full shape of one trip (route).
    From the resulting shape we then obtain only shape points between
    stop_seq1 and stop_seq2
    trip_I---(trips)--->shape_id
    trip_I, seq_stop1----(stop_times)---> shape_break1
    trip_I, seq_stop2----(stop_times)---> shape_break2
    shapes_id+shape_break1+shape_break2 --(shapes)--> result

    Parameters
    ----------
    cur : sqlite3.Cursor
        cursor to sqlite3 DB containing GTFS
    trip_I : int
        transformed trip_id (i.e. a new column that is created when
        GTFS is imported to a DB)
    seq_stop1: int
        a positive inger describing the index of the point of the shape that
        corresponds to the first stop
    seq_stop2: int
        a positive inger describing the index of the point of the shape that
        corresponds to the second stop
    shape_breaks: ??

    Returns
    -------
    shapedict: dict
        Dictionary containing the latitudes and longitudes:
            lats=shapedict['lat']
            lons=shapedict['lon']
    """

    assert (seq_stop1 and seq_stop2) or shape_breaks
    if not shape_breaks:
        shape_breaks = []
        for seq_stop in [seq_stop1, seq_stop2]:
            query = """SELECT shape_break FROM stop_times
                        WHERE trip_I=%d AND seq=%d
                    """ % (trip_I, seq_stop)
            for row in cur.execute(query):
                shape_breaks.append(row[0])
    assert len(shape_breaks) == 2

    query = """SELECT seq, lat, lon
                FROM (SELECT shape_id FROM trips WHERE trip_I=%d)
                JOIN shapes USING (shape_id)
                WHERE seq>=%d AND seq <= %d;
            """ % (trip_I, shape_breaks[0], shape_breaks[1])
    shapedict = {'lat': [], 'lon': [], 'seq': []}
    for row in cur.execute(query):
        shapedict['seq'].append(row[0])
        shapedict['lat'].append(row[1])
        shapedict['lon'].append(row[2])
    return shapedict


def get_trip_points(cur, route_id, offset=0, tripid_glob=''):
    """Get all scheduled stops on a particular route_id.

    Given a route_id, return the trip-stop-list with
    latitude/longitudes.  This is a bit more tricky than it seems,
    because we have to go from table route->trips->stop_times.  This
    functions finds an arbitrary trip (in trip table) with this route ID
    and, and then returns all stop points for that trip.

    Parameters
    ----------
    cur : sqlite3.Cursor
        cursor to sqlite3 DB containing GTFS
    route_id : string or any
        route_id to get stop points of
    offset : int
        LIMIT offset if you don't want the first trip returned.
    tripid_glob : string
        If given, allows you to limit tripids which can be selected.
        Mainly useful in debugging.

    Returns
    -------
    stop-list
        List of stops in stop-seq format.
    """
    extra_where = ''
    if tripid_glob:
        extra_where = "AND trip_id GLOB '%s'" % tripid_glob
    cur.execute('SELECT seq, lat, lon '
                'FROM (select trip_I from route '
                '      LEFT JOIN trips USING (route_I) '
                '      WHERE route_id=? %s limit 1 offset ? ) '
                'JOIN stop_times USING (trip_I) '
                'LEFT JOIN stop USING (stop_id) '
                'ORDER BY seq' % extra_where, (route_id, offset))
    stop_points = [dict(seq=row[0], lat=row[1], lon=row[2]) for row in cur]
    return stop_points


def interpolate_shape_times(shape_distances, shape_breaks, stop_times):
    """
    Interpolate passage times for shape points.

    Parameters
    ----------
    shape_distances: list
        list of cumulative distances along the shape
    shape_breaks: list
        list of shape_breaks
    stop_times: list
        list of stop_times

    Returns
    -------
    shape_times: list of ints (seconds) / numpy array
        interpolated shape passage times

    The values of stop times before the first shape-break are given the first
    stopping time, and the any shape points after the last break point are
    given the value of the last shape point.
    """
    shape_times = np.zeros(len(shape_distances))
    shape_times[:shape_breaks[0]] = stop_times[0]
    for i in range(len(shape_breaks)-1):
        cur_break = shape_breaks[i]
        cur_time = stop_times[i]
        next_break = shape_breaks[i+1]
        next_time = stop_times[i+1]
        if cur_break == next_break:
            shape_times[cur_break] = stop_times[i]
        else:
            cur_distances = shape_distances[cur_break:next_break+1]
            norm_distances = ((np.array(cur_distances)-float(cur_distances[0])) /
                              float(cur_distances[-1] - cur_distances[0]))
            times = (1.-norm_distances)*cur_time+norm_distances*next_time
            shape_times[cur_break:next_break] = times[:-1]
    # deal final ones separately:
    shape_times[shape_breaks[-1]:] = stop_times[-1]
    return list(shape_times)
