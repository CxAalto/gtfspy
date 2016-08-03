import util
from util import wgs84_distance
import gtfs
import pandas as pd
from collections import namedtuple

import numpy as np
from heapq import heappush, heappushpop, heappop

def get_spreading_trips(conn, start_time_ut, lat, lon,
                        max_duration_ut=4*3600,
                        min_transfer_time=30):
    """
    Starting from a specific point and time, get complete single source
    shortest path spreading dynamics using trip notation.

    Parameters
    ----------
    conn: sqlite3.Connection
        A database connection cursor.
    start_time_ut: number
        Start time of the spreading.
    lat: float
        latitude of the spreding seed location
    lon: float
        longitude of the spreading seed location
    max_duration_ut: int
    min_transfert_time : int
        minimum transfer time in seconds

    Returns
    -------
    trips: dict
        trips['trips'] is a list whose each element (e.g. el = trips['trips'][0])
        is a dict with the following properties:
            el['lats'] : list of latitudes
            el['lons'] : list of longitudes
            el['times'] : list of passage_times
            el['route_type'] : type of vehicle as specified by GTFS, or -1 if walking
            el['name'] : name of the route
    """

    # events are sorted by arrival time, so in order to use the
    # heapq, we need to have events coded as
    # (arrival_time, (from_stop, to_stop))
    start_stop_I = gtfs.get_closest_stop(conn.cursor(), lat, lon)
    end_time_ut = start_time_ut + max_duration_ut

    print "Computing/fetching events"
    events_df = gtfs.get_events_within_range(conn, start_time_ut, start_time_ut+ max_duration_ut)
    # events_df.sort('arr_time_ut', inplace=True)
    from_stop_Is = set(events_df['from_stop_I'])
    to_stop_Is = set(events_df['to_stop_I'])

    infected_stops = set([start_stop_I])
    all_stops = set(gtfs.get_stop_data(conn)['stop_I'])

    uninfected_stops = all_stops.copy()
    uninfected_stops.remove(start_stop_I)

    # match stop_I to a more advanced stop object
    seed_stop = SpreadingStop(start_stop_I, min_transfer_time)

    stop_I_to_spreading_stop = {
        start_stop_I: seed_stop
    }
    for stop in uninfected_stops:
        stop_I_to_spreading_stop[stop] = SpreadingStop(stop, min_transfer_time)

    # get for each stop their

    walk_speed = 0.5 #meters/second

    print "intializing heap"
    event_heap = Heap(events_df)

    start_event = Event(start_time_ut-1,
                        start_time_ut-1,
                        start_stop_I,
                        start_stop_I,
                        -1)

    seed_stop.visit(start_event)
    assert len(seed_stop.visit_events) > 0
    event_heap.add_event(start_event)
    event_heap.add_walk_events_to_heap(conn, start_event, start_time_ut, walk_speed, uninfected_stops, max_duration_ut)

    i=1

    while event_heap.size() > 0 and len(uninfected_stops) > 0:
        e = event_heap.pop_next_event()
        this_stop = stop_I_to_spreading_stop[e.from_stop_I]

        if e.arr_time_ut > start_time_ut+max_duration_ut:
            break

        if this_stop.can_infect(e):

            target_stop = stop_I_to_spreading_stop[e.to_stop_I]
            already_visited = target_stop.has_been_visited()
            target_stop.visit(e)

            if not already_visited:
                uninfected_stops.remove(e.to_stop_I)
                print i, event_heap.size()
                event_heap.add_walk_events_to_heap(conn, e, start_time_ut, walk_speed, uninfected_stops, max_duration_ut)
                i+=1

    # create new transfer events and add them to the heap (=queue)
    inf_times = [[stop_I, el.get_min_visit_time()-start_time_ut] \
                    for stop_I, el in stop_I_to_spreading_stop.items()]
    inf_times = np.array(inf_times)
    inf_time_data = pd.DataFrame(inf_times, columns=["stop_I", "inf_time_ut"])
    stop_data = gtfs.get_stop_data(conn)

    # join latitudes and longitudes to get something plottable on a map
    stop_Is, lats, lons = stop_data.stop_I, stop_data.lat, stop_data.lon

    combined = inf_time_data.merge(stop_data, how='inner', on='stop_I', suffixes=('_infs', '_stops'), copy=True)

    trips = []
    for stop_I, dest_stop_obj in stop_I_to_spreading_stop.iteritems():
        inf_event = dest_stop_obj.get_min_event()
        if inf_event is None:
            continue

        dep_stop_I = inf_event.from_stop_I
        dep_lat = float(combined[combined['stop_I']==dep_stop_I]['lat'].values)
        dep_lon = float(combined[combined['stop_I']==dep_stop_I]['lon'].values)

        dest_lat = float(combined[combined['stop_I']==stop_I]['lat'].values)
        dest_lon = float(combined[combined['stop_I']==stop_I]['lon'].values)

        if inf_event.trip_I == -1:
            name = "walk"
            rtype = -1
        else:
            name, rtype = \
                gtfs.get_route_name_and_type(conn.cursor(), inf_event.trip_I)

        trip = {
            "lats": [dep_lat, dest_lat],
            "lons": [dep_lon, dest_lon],
            "times": [inf_event.dep_time_ut, inf_event.arr_time_ut],
            "name": name,
            "route_type": rtype
        }
        trips.append(trip)

    return {"trips":trips}

    # print combined.columns
    # import matplotlib.pyplot as plt
    # fig, ax = plt.subplots()
    # ax.scatter(combined.lon, combined.lat, s=40, c=np.log(combined.inf_time_ut), cmap="summer")
    # # trips =
    # plt.show()


Event = namedtuple('Event', ['arr_time_ut', 'dep_time_ut', 'from_stop_I', 'to_stop_I', 'trip_I'])


class Heap:

    def __init__(self, pd_df=None):
        self.heap = []
        keys = ['arr_time_ut', 'dep_time_ut', 'from_stop_I', 'to_stop_I', 'trip_I']

        # pd_df.iterrows() is slow as it creates new Series objects!
        n = len(pd_df)
        key_to_j = {}
        for j, key in enumerate(pd_df.columns.values):
            key_to_j[key] = j
        pd_df_values = pd_df.values
        for i in range(n):
            vals = []
            for key in keys:
                j = key_to_j[key]
                vals.append(pd_df_values[i, j])
            e = Event(*vals)
            self.add_event(e)

    def add_event(self, event):
        """
        Add an event to the heap/priority queue

        Parameters
        ----------
        event : Event
        """
        assert event.dep_time_ut <= event.arr_time_ut
        heappush(self.heap, event)


    def pop_next_event(self):
        return heappop(self.heap)

    def size(self):
        """
        Return the size of the heap
        """
        return len(self.heap)

    def add_walk_events_to_heap(self, conn, e, start_time_ut, walk_speed, uninfected_stops, max_duration_ut):
        dists = gtfs.get_distances(conn, e.to_stop_I)
        n = len(dists)
        dists_values = dists.values
        #
        to_stop_I_index = np.nonzero(dists.columns=='to_stop_I')[0][0]
        d_index = np.nonzero(dists.columns=='d')[0][0]
        for i in range(n):
            transfer_to_stop_I = dists_values[i,to_stop_I_index]
            if transfer_to_stop_I in uninfected_stops:
                d = dists_values[i, d_index]
                transfer_arr_time = e.arr_time_ut + int(d/float(walk_speed))
                if transfer_arr_time > start_time_ut+max_duration_ut:
                    continue
                # trip_i = -1 for walking
                te = Event(transfer_arr_time, e.arr_time_ut, e.to_stop_I, transfer_to_stop_I, -1)
                self.add_event(te)


class SpreadingStop:

    def __init__(self, stop_I, min_transfer_time):
        self.stop_I = stop_I
        self.min_transfer_time = min_transfer_time
        self.visit_events = []

    def get_min_visit_time(self):
        """
        Get the earliest visit time of the stop.
        """
        if not self.visit_events:
            return float('inf')
        else:
            return min(self.visit_events, key=lambda event: event.arr_time_ut).arr_time_ut

    def get_min_event(self):
        if len(self.visit_events) == 0:
            return None
        else:
            return min(self.visit_events, key=lambda event: event.arr_time_ut)

    def visit(self, event):
        """
        Visit the stop if it has not been visited already by an event with
        earlier arr_time_ut (or with other trip that does not require a transfer)

        Parameters
        ----------
        arr_time_ut: int
            arrival time in unix time (seconds)
        event : int
            an instance of the Event (namedtuple)

        Returns
        -------
        visited : bool
            if visit is stored, returns True, otherwise False
        """
        to_visit = False
        if event.arr_time_ut <= self.min_transfer_time+self.get_min_visit_time():
            to_visit = True
        else:
            for ve in self.visit_events:
                if (event.trip_I == ve.trip_I) and event.arr_time_ut < ve.arr_time_ut:
                    to_visit = True

        if to_visit:
            self.visit_events.append(event)
            min_time = self.get_min_visit_time()
            # remove any visits that are 'too old'
            self.visit_events = [v for v in self.visit_events if v.arr_time_ut <= min_time+self.min_transfer_time]
        return to_visit

    def has_been_visited(self):
        return len(self.visit_events) > 0

    def can_infect(self, event):
        """
        Whether the spreading stop can infect using this event.
        """
        if event.from_stop_I != self.stop_I:
            return False

        if not self.has_been_visited():
            return False
        else:
            time_sep = event.dep_time_ut-self.get_min_visit_time()
            # if the gap between the earliest visit_time and current time is
            # smaller than the min. transfer time, the stop can pass the spreading
            # forward
            if (time_sep >= self.min_transfer_time) or (event.trip_I==-1 and time_sep>=0):
                return True
            else:
                for visit in self.visit_events:
                    # if no transfer, please hop-on
                    if (event.trip_I == visit.trip_I) and (time_sep >= 0):
                        return True
            return False






