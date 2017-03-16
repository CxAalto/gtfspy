from __future__ import absolute_import, print_function

import numpy
import pandas as pd

from gtfspy.gtfs import GTFS
from .event import Event
from .heap import EventHeap
from .spreading_stop import SpreadingStop


class Spreader(object):
    """
    Starting from a specific point and time, get complete single source
    shortest path spreading dynamics as trips, or "events".
    """

    def __init__(self, gtfs, start_time_ut, lat, lon, max_duration_ut, min_transfer_time=30,
                 shapes=True, walk_speed=0.5):
        """
        Parameters
        ----------
        gtfs: GTFS
            the underlying GTFS (database) connection for getting data
        start_time_ut: number
            Start time of the spreading.
        lat: float
            latitude of the spreading seed location
        lon: float
            longitude of the spreading seed location
        max_duration_ut: int
            maximum duration of the spreading process (in seconds)
        min_transfer_time : int
            minimum transfer time in seconds
        shapes : bool
            whether to include shapes
        """
        self.gtfs = gtfs
        self.start_time_ut = start_time_ut
        self.lat = lat
        self.lon = lon
        self.max_duration_ut = max_duration_ut
        self.min_transfer_time = min_transfer_time
        self.shapes = shapes
        self.event_heap = None
        self.walk_speed = walk_speed
        self._uninfected_stops = None
        self._stop_I_to_spreading_stop = None
        self._initialized = False
        self._has_run = False

    def spread(self):
        self._initialize()
        self._run()
        return self._get_shortest_path_trips()

    def _initialize(self):
        if self._initialized:
            raise RuntimeError("This spreader instance has already been initialized: "
                               "create a new Spreader object for a new run.")
        # events are sorted by arrival time, so in order to use the
        # heapq, we need to have events coded as
        # (arrival_time, (from_stop, to_stop))
        start_stop_I = self.gtfs.get_closest_stop(self.lat, self.lon)
        end_time_ut = self.start_time_ut + self.max_duration_ut

        print("Computing/fetching events")
        events_df = self.gtfs.get_transit_events(self.start_time_ut, end_time_ut)
        all_stops = set(self.gtfs.stops()['stop_I'])

        self._uninfected_stops = all_stops.copy()
        self._uninfected_stops.remove(start_stop_I)

        # match stop_I to a more advanced stop object
        seed_stop = SpreadingStop(start_stop_I, self.min_transfer_time)

        self._stop_I_to_spreading_stop = {
            start_stop_I: seed_stop
        }
        for stop in self._uninfected_stops:
            self._stop_I_to_spreading_stop[stop] = SpreadingStop(stop, self.min_transfer_time)

        # get for each stop their
        print("intializing heap")
        self.event_heap = EventHeap(events_df)

        start_event = Event(self.start_time_ut - 1,
                            self.start_time_ut - 1,
                            start_stop_I,
                            start_stop_I,
                            -1)

        seed_stop.visit(start_event)
        assert len(seed_stop.visit_events) > 0
        self.event_heap.add_event(start_event)
        transfer_distances = self.gtfs.get_straight_line_transfer_distances(start_event.to_stop_I)
        self.event_heap.add_walk_events_to_heap(
            transfer_distances,
            start_event,
            self.start_time_ut,
            self.walk_speed,
            self._uninfected_stops,
            self.max_duration_ut
        )
        self._initialized = True

    def _run(self):
        """
        Run the actual simulation.
        """
        if self._has_run:
            raise RuntimeError("This spreader instance has already been run: "
                               "create a new Spreader object for a new run.")
        i = 1
        while self.event_heap.size() > 0 and len(self._uninfected_stops) > 0:
            event = self.event_heap.pop_next_event()
            this_stop = self._stop_I_to_spreading_stop[event.from_stop_I]

            if event.arr_time_ut > self.start_time_ut + self.max_duration_ut:
                break

            if this_stop.can_infect(event):

                target_stop = self._stop_I_to_spreading_stop[event.to_stop_I]
                already_visited = target_stop.has_been_visited()
                target_stop.visit(event)

                if not already_visited:
                    self._uninfected_stops.remove(event.to_stop_I)
                    print(i, self.event_heap.size())
                    transfer_distances = self.gtfs.get_straight_line_transfer_distances(event.to_stop_I)
                    self.event_heap.add_walk_events_to_heap(transfer_distances, event, self.start_time_ut,
                                                            self.walk_speed, self._uninfected_stops,
                                                            self.max_duration_ut)
                    i += 1
        self._has_run = True

    def _get_shortest_path_trips(self):
        """
        Returns
        -------
        trips: list
            trips['trips'] is a list whose each element (e.g. el = trips['trips'][0])
            is a dict with the following properties:
                el['lats'] : list of latitudes
                el['lons'] : list of longitudes
                el['times'] : list of passage_times
                el['route_type'] : type of vehicle as specified by GTFS (and -1 for walking)
                el['name'] : name of the route
        """
        if not self._has_run:
            raise RuntimeError("This spreader object has not run yet. Can not return any trips.")
        # create new transfer events and add them to the heap (=queue)
        inf_times = [[stop_I, el.get_min_visit_time() - self.start_time_ut]
                     for stop_I, el in self._stop_I_to_spreading_stop.items()]
        inf_times = numpy.array(inf_times)
        inf_time_data = pd.DataFrame(inf_times, columns=["stop_I", "inf_time_ut"])
        stop_data = self.gtfs.stops()

        combined = inf_time_data.merge(stop_data, how='inner', on='stop_I', suffixes=('_infs', '_stops'), copy=True)

        trips = []
        for stop_I, dest_stop_obj in self._stop_I_to_spreading_stop.items():
            inf_event = dest_stop_obj.get_min_event()
            if inf_event is None:
                continue
            dep_stop_I = inf_event.from_stop_I
            dep_lat = float(combined[combined['stop_I'] == dep_stop_I]['lat'].values)
            dep_lon = float(combined[combined['stop_I'] == dep_stop_I]['lon'].values)

            dest_lat = float(combined[combined['stop_I'] == stop_I]['lat'].values)
            dest_lon = float(combined[combined['stop_I'] == stop_I]['lon'].values)

            if inf_event.trip_I == -1:
                name = "walk"
                rtype = -1
            else:
                name, rtype = self.gtfs.get_route_name_and_type_of_tripI(inf_event.trip_I)

            trip = {
                "lats"      : [dep_lat, dest_lat],
                "lons"      : [dep_lon, dest_lon],
                "times"     : [inf_event.dep_time_ut, inf_event.arr_time_ut],
                "name"      : name,
                "route_type": rtype
            }
            trips.append(trip)
        return {"trips": trips}
