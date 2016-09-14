"""
Author: rmkujala
"""
from collections import defaultdict
from collections import namedtuple
import time

import networkx

Connection = namedtuple('Connection',
                        ['departure_stop', 'arrival_stop', 'departure_time', 'arrival_time', 'trip_id'])


class ConnectionScan:
    """
    A simple implementation of the Connection Scan Algorithm (CSA) for public transport networks that fits
    (relatively well) the gtfspy data model.
    """

    def __init__(self, transit_events, seed_stop, start_time,
                 end_time, transfer_margin, walk_network, walk_speed):
        """
        Parameters
        ----------
        transit_events: list[Connection]
        seed_stop: int
            index of the seed node
        start_time : int
            start time in unixtime seconds
        end_time: int
            end time in unixtime seconds
        transfer_margin: int
            required extra margin required for transfers in seconds
        walk_speed: float
            walking speed between stops in meters / second
        walk_network: networkx.Graph
            each edge should have the walking distance as an attribute ("distance_shape") expressed in meters
        """
        self._seed = seed_stop
        self._connections = transit_events
        self._start_time = start_time
        self._end_time = end_time
        self._transfer_margin = transfer_margin
        self._walk_network = walk_network
        self._walk_speed = walk_speed

        # algorithm internals
        self.__stop_labels = defaultdict(lambda: float('inf'))
        self.__stop_labels[seed_stop] = start_time

        # trip flags:
        self.__trip_reachable = defaultdict(lambda: False)

        # meta information:
        self._run_time = None
        self._has_run = False

    def run(self):
        """Run the algorithm"""
        if self._has_run:
            raise RuntimeError("Algorithm has already run, please initialize a new algorithm")
        start_time = time.time()
        self._run()
        end_time = time.time()
        self._run_time = end_time - start_time
        self._has_run = True

    def get_arrival_times(self):
        """
        Returns
        -------
        arrival_times: dict[int, float]
            maps integer stop_ids to floats
        """
        assert self._has_run
        return self.__stop_labels

    def get_run_time(self):
        """
        Returns
        -------
        run_time: float
            running time of the algorithm in seconds
        """
        assert self._has_run
        return self._run_time

    def _run(self):
        for connection in self._connections:
            from_stop = connection.departure_stop
            to_stop = connection.arrival_stop
            departure_time = connection.departure_time
            arrival_time = connection.arrival_time
            trip_id = connection.trip_id
            reachable = False
            if self.__trip_reachable[trip_id]:
                reachable = True
            else:
                dep_stop_reached = self.__stop_labels[from_stop]
                if dep_stop_reached + self._transfer_margin <= departure_time:
                    self.__trip_reachable[trip_id] = True
                    reachable = True
            if reachable:
                self._update_stop_label(to_stop, arrival_time)
                self._scan_footpaths(to_stop, arrival_time)

    def _update_stop_label(self, stop, arrival_time):
        current_stop_label = self.__stop_labels[stop]
        if current_stop_label > arrival_time:
            self.__stop_labels[stop] = arrival_time

    def _scan_footpaths(self, stop_id, walk_departure_time):
        """
        Scan the footpaths originating from stop_id

        Parameters
        ----------
        stop_id: int
        """
        for _, neighbor, distance_shape in self._walk_network.edges_iter(nbunch=[stop_id], data="distance_shape"):
            arrival_time = walk_departure_time + distance_shape / self._walk_speed
            self._update_stop_label(neighbor, arrival_time)


