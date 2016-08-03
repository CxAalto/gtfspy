import json
import sys

import db
import shapes

import numpy as np

def to_json(x):
    return json.dumps(x)



def get_trips(start, end, limit=None, interval='arr_time', bus_lines=None):
    """Get complete trip data as delay viz.

    Parameters
    ----------
    start: number
        Earliest position data to return (usually unix time)
    end: number
        Latest position data to return (usually unix time)
    limit: int, optional
        If given, return only this many trips. Useful only for testing
        with smaller amount of data.
    interval: str, optional.
        Mode for selecting time intervals.  Either 'arr_time' or
        'run_schstarttime'.  Default: 'arr_time' and this is what makes
        the most sense in general.
    bus_lines: iterable of str, optional
        If given, return only trips of these vehicles.  Untested and
        should be considered TODO.
    """

    # Connect to DB.  TODO: make more general.
    conn = db.connect_gps(name='2015-03-01', gtfs='hsl-2015-04-24')
    cur = conn.cursor()
    cur2 = conn.cursor()

    # Below, we build up the where_clause that is used to select
    # trips.  The primary purpose is to limit our data to that only
    # within a certain interval.

    # There are two ways to select trips by time.
    if interval == 'arr_time':
        # The first one, using the "arr_time" column, gets all
        # vehicles that are moving within an interval.
        where_clause = '?<=arr_time and arr_time < ?'
    else:
        # The second, using the "run_schstarttime" column, gets only
        # vehicles that _start_ their trip in the interval.  If a bus
        # was already running at tstart, then it would not be included
        # in the second version.
        where_clause = '?<=run_sch_starttime and run_sch_starttime < ?'
    # Some logic for selecting only certain bus lines.  This is
    # hackish and not used anywhere (if it was used, it should be
    # re-written).
    if bus_lines:
        where_clause_2 = '(' + ' or '.join("routes.name='%s'"%r for r in bus_lines) + ')'
    else:
        where_clause_2 = '1'
    #print where_clause_2


    # Find the trip IDs that exist within our chosen time interval
    # (where_clause).  The key is (run_code, run_sch_starttime).
    cur.execute('''SELECT distinct run_code, run_sch_starttime
                   FROM gps LEFT JOIN routes USING (route_id)
                   WHERE %s and %s
                   ORDER BY run_sch_starttime'''%(where_clause, where_clause_2),
                (start, end))
    codes = cur.fetchall()
    # Limit amount of data returned (number of trips).
    if limit:
        codes = codes[:limit]

    # This is used to save re-computing shapes so often.
    shape_cache = { }

    # The main loop that iterates through all relevant
    # .
    # For every trip key, get the nodes and add it to
    # the trips list.
    trips = [ ]
    for run_code, run_sch_starttime in codes:

        this_trip = { }
        # Get metadata on the trip.
        name, route_id, shape_id \
           = cur2.execute("""SELECT name, route_id, shape_id
                             FROM gps_meta LEFT JOIN routes USING (route_id)
                             WHERE run_code=? AND run_sch_starttime=?""",
                          (run_code, run_sch_starttime)).fetchone()
        # Set up the data structure that will hold all the trip values.
        this_trip['trip_id'] = '%s-%s'%(run_code,run_sch_starttime)
        this_trip['name'] = name
        this_trip['arr_times'] = [ ]
        this_trip['sch_times'] = [ ]
        this_trip['delays'] = [ ]
        this_trip['lats'] = [ ]
        this_trip['lons'] = [ ]
        breakpoints = [ ]  # The shape index of each stop.

        # Get the actual trip data.  This consists of one row for
        # every stop the vehicles passes.
        cur2.execute('''SELECT arr_time, delay, lat, lon, sch_time, shape_break
                        FROM gps JOIN stops USING (stop_id)
                        WHERE run_code=? AND run_sch_starttime=?
                            AND %s
                        ORDER BY arr_time'''%where_clause,
                     (run_code, run_sch_starttime, start, end))
        # Trivially unpack the SQL data into the data structure above.
        for row in cur2:
            this_trip['arr_times'].append(row[0])
            this_trip['delays'].append(row[1])
            this_trip['lats'].append(row[2])
            this_trip['lons'].append(row[3])
            this_trip['sch_times'].append(row[4])
            breakpoints.append(row[5])

        # Sanity check for data.  If it is bad (unrealistically
        # delayed), then don't use it.
        if any(x>3600 for x in this_trip['delays']):
            #print this_trip
            #import numpy
            #print numpy.subtract(this_trip['arr_times'], this_trip['sch_times'])
            continue

        # Go through and compute the shapes for each trip.  We got
        # shape_id in the metadaata above.  We add a "waypoints"
        # element to our trip data dictonary.
        if shape_id is not None:
            # shape_cache is used to hold the shape data (key:
            # shape_id) so that we don't have to select it from the DB
            # every time.  Saves some compute time and I/O.
            if shape_id not in shape_cache:
                # Data not found in cache: use shapes.get_shape_points().
                shape_points = shapes.get_shape_points(cur2, shape_id)
                shape_cache[shape_id] = shape_points
            else:
                # Found in cache.
                shape_points = shape_cache[shape_id]
            # shapes.return_segments converts uses breakpoints (the
            # list of "which shape index corresponds to each stop") to
            # make a list of separate sequences from one stop to the
            # next.
            segs = shapes.return_segments(shape_points, breakpoints)
            # Rearrange the dicts from [ dict(lat=N, lon=N, d=d), ...]
            # to dict(lat=[N,N,...], lon=[N,N,...], d=[d,d,...]).  Do
            # this for every segment within the waypoints list.
            this_trip['waypoints'] = [
                dict(lat=[row['lat'] for row in seg ],
                     lon=[row['lon'] for row in seg ],
                     d=[row['d'] for row in seg ])
                for seg in segs
                ]

        # Rearrange the above data into the new format.
        tt = this_trip

        new_trip = {
            'lats': [],
            'lons': [],
            'times': [],
            'delays': [],
            'name': tt['name'],
            'id': tt['trip_id']
        }

        # For a trip, interpolate passage times, and delays based on
        # the waypoints and especially using the cumultive distances
        # wps['d']
        loopdata = zip(tt['arr_times'],
                       tt['lats'],
                       tt['lons'],
                       tt['waypoints'],
                       tt['delays']
                    )
        for i, row in enumerate(loopdata):
            (arr_time, lat, lon, wps, delay) = row
            if len(wps['lat']) > 2:
                # in case there are actual waypoints:

                # add stop latitude:
                new_trip['lats'].extend(wps['lat'])
                new_trip['lons'].extend(wps['lon'])

                #
                delay_start = delay
                delay_end = tt['delays'][i+1]
                time_start = arr_time
                time_end = tt['arr_times'][i+1]

                norm_cum_dists = (np.array(wps['d'])-wps['d'][0])/float(wps['d'][-1]-wps['d'][0])

                times = time_start + norm_cum_dists*(time_end-time_start)
                delays = delay_start + norm_cum_dists*(delay_end-delay_start)

                new_trip['times'].extend(times)
                new_trip['delays'].extend(delays)
            else:
                # if no waypoints are available:
                new_trip['lats'].append(lat)
                new_trip['lons'].append(lon)
                new_trip['delays'].append(delay)
                new_trip['times'].append(arr_time)


        trips.append(new_trip)
        # for i in range(len(new_trip['times'])-1):
        #     assert (new_trip['times'][i+1] >= new_trip['times'][i])


    return dict(trips=trips)


def spreading_gen(start_stop, start_time):
    """Simulate a spreading process on the temporal network.

    This is unfinished and probably won't even work with current databases.
    """
    # find the first tnode.
    conn = db.get_db()
    cur = conn.cursor()
    cur2 = conn.cursor()

    max_distance = 500
    max_time = 600


    cur.execute('''select sid2 from stop_distances where sid1=? and distance<?''',
                (start_stop, max_distance))
    nearby_stops = cur.fetchall()
    if nearby_stops: nearby_stops = list(zip(*nearby_stops)[0])
    nearby_stops.insert(0, start_stop)
    print >> sys.stderr, nearby_stops

    from heapq import heappush, heappop
    next_steps = [ ]
    seen_stops = { }

    path = [ ]

    # Initial build-up of all busses leaving within some period of time of our stop.
    for sid in nearby_stops:
        cur.execute('''SELECT tnid1, time1, lat, lon
                       FROM tnet_actual LEFT JOIN stop ON (stop1=sid)
                   WHERE stop1=? AND time1>=? AND type=0
                   ORDER BY time1 ASC''', (sid, start_time))
        for tnid1, time1, lat, lon in cur:
            data = dict(transfers=0, lat=lat, lon=lon)
            heappush(next_steps, (time1, tnid1, data))  # time, tnid, transsfers

    while next_steps:
        time1, tnid1, prev_data = heappop(next_steps)
        cur.execute('''SELECT tnid2, time2, stop2, type, lat, lon, code
                       FROM tnet_actual LEFT JOIN stop ON (stop2=sid)
                   WHERE tnid1=? AND time1>=? -- AND type=0
                   ORDER BY time1 ASC''', (tnid1, start_time))
        for tnid2, time2, stop2, is_transfer, lat, lon, code in cur:
            if stop2 in seen_stops:
                continue
            seen_stops[stop2] = time2

            data = dict(transfers=prev_data['transfers']+is_transfer,
                        lat=lat, lon=lon)
            heappush(next_steps, (time2, tnid2, data))

            d = dict(tnid2=tnid2, time=time2,
                     lat=lat, lon=lon,
                     prev_lat=prev_data['lat'], prev_lon=prev_data['lon'],
                     dt=time2-start_time,
                     transfers=data['transfers'])
            print d
            yield d

def spreading(start_time, start_stop):

    data = [ ]

    for d in spreading_gen(start_time, start_stop):
        data.append(d)
    return dict(points=data)



if __name__ == "__main__":
    data = get_trips(1425183840, 1425183840+3600*1)
    print type(data)
    print data['trips'][0].keys()
    # data = get_trips(1425186000, 1425189600, bus_lines=[195])
    #print json.dumps(data)

    # data = spreading(2222227, 1425193200)
    #print json.dumps(data)
