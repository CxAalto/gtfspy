"""
Author: rmkujala
"""

from collections import defaultdict

import networkx

from gtfspy.routing.abstract_routing_algorithm import AbstractRoutingAlgorithm


class ConnectionScan(AbstractRoutingAlgorithm):
    """
    A simple implementation of the Connection Scan Algorithm (CSA) solving the first arrival problem
    for public transport networks.

    http://i11www.iti.uni-karlsruhe.de/extra/publications/dpsw-isftr-13.pdf
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
            end time in unixtime seconds (no new connections will be scanned after this time)
        transfer_margin: int
            required extra margin required for transfers in seconds
        walk_speed: float
            walking speed between stops in meters / second
        walk_network: networkx.Graph
            each edge should have the walking distance as a data attribute ("d_walk") expressed in meters
        """
        AbstractRoutingAlgorithm.__init__(self)
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

    def get_arrival_times(self):
        """
        Returns
        -------
        arrival_times: dict[int, float]
            maps integer stop_ids to floats
        """
        assert self._has_run
        return self.__stop_labels

    def _run(self):
        self._scan_footpaths(self._seed, self._start_time)
        for connection in self._connections:
            departure_time = connection.departure_time
            if departure_time > self._end_time:
                return
            from_stop = connection.departure_stop
            to_stop = connection.arrival_stop
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
        for _, neighbor, data in self._walk_network.edges(nbunch=[stop_id], data=True):
            d_walk = data["d_walk"]
            arrival_time = walk_departure_time + d_walk / self._walk_speed
            self._update_stop_label(neighbor, arrival_time)


