from heapq import heappush, heappop

import numpy as np

from ..gtfs import GTFS
from ..route_types import WALK
from .event import Event

class EventHeap:
    """
    EventHeap represents a container for the event
    heap to run time-dependent Dijkstra for public transport routing objects.
    """

    def __init__(self, pd_df=None):
        """
        Parameters
        ----------
        pd_df : Pandas.Dataframe
            Initial list of
        """
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

    def add_walk_events_to_heap(self, transfer_distances, e, start_time_ut, walk_speed, uninfected_stops, max_duration_ut):
        """
        Parameters
        ----------
        transfer_distances:
        e : Event
        start_time_ut : int
        walk_speed : float
        uninfected_stops : list
        max_duration_ut : int
        """
        n = len(transfer_distances)
        dists_values = transfer_distances.values
        to_stop_I_index = np.nonzero(transfer_distances.columns == 'to_stop_I')[0][0]
        d_index = np.nonzero(transfer_distances.columns == 'd')[0][0]
        for i in range(n):
            transfer_to_stop_I = dists_values[i, to_stop_I_index]
            if transfer_to_stop_I in uninfected_stops:
                d = dists_values[i, d_index]
                transfer_arr_time = e.arr_time_ut + int(d/float(walk_speed))
                if transfer_arr_time > start_time_ut+max_duration_ut:
                    continue
                te = Event(transfer_arr_time, e.arr_time_ut, e.to_stop_I, transfer_to_stop_I, WALK)
                self.add_event(te)
