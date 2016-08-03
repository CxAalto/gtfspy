# if gtfs.py is imported here, if gtfs.py is run as the main script,
# then we get a cyclic import error and gtfs.py can't import Heap,
# Event, and SpreadingStop directly from here.
#import gtfs
from collections import namedtuple

import numpy as np
from heapq import heappush, heappushpop, heappop


"""
This module contains utility classes for function modules.
"""

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

    def add_walk_events_to_heap(self, G, e, start_time_ut, walk_speed, uninfected_stops, max_duration_ut):
        dists = G.get_straight_line_transfer_distances(e.to_stop_I)
        n = len(dists)
        dists_values = dists.values
        #
        to_stop_I_index = np.nonzero(dists.columns == 'to_stop_I')[0][0]
        d_index = np.nonzero(dists.columns == 'd')[0][0]
        for i in range(n):
            transfer_to_stop_I = dists_values[i, to_stop_I_index]
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
        event : Event
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
            if (time_sep >= self.min_transfer_time) or (event.trip_I == -1 and time_sep >= 0):
                return True
            else:
                for visit in self.visit_events:
                    # if no transfer, please hop-on
                    if (event.trip_I == visit.trip_I) and (time_sep >= 0):
                        return True
            return False
