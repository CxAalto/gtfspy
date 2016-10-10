"""
An implementation of the profile connection scan algorithm.

Problem description:
Given
1. a static network for pedestrian routing
2. a temporal network with elementary transit events (accompanied with trip_ids)
3. source stop
4. target stop
5. interval start time
6. interval end time

Compute the pareto optimal departure times (from the source stop) and arrival times (to the target stop).
Considering pareto-optimality the following are beneficial
LATER (greater) departure time
EARLIER (smaller) arrival time

Now, the following departure_time, arrival_time pairs would all be pareto-optimal:
1, 3
3, 4
4, 5

However, e.g. (2, 4) would not be a pareto-optimal (departure_time, arrival_time) pair as it is dominated by (4,5)

while only one link in the static network can be traversed at a time.

Implements
"""
from collections import defaultdict

import networkx

from gtfspy.routing.models import Connection, ParetoTuple
from gtfspy.routing.node_profile import NodeProfile, IdentityNodeProfile, DecreasingDepTimeNodeProfile
from gtfspy.routing.abstract_routing_algorithm import AbstractRoutingAlgorithm


class ConnectionScanProfiler(AbstractRoutingAlgorithm):
    """
    Implementation of the profile connection scan algorithm presented in

    http://i11www.iti.uni-karlsruhe.de/extra/publications/dpsw-isftr-13.pdf
    """

    def __init__(self,
                 transit_events,
                 target_stop,
                 start_time=None,
                 end_time=None,
                 transfer_margin=0,
                 walk_network=None,
                 walk_speed=1.5,
                 verbose=False,
                 node_profile_class=NodeProfile):
        """
        Parameters
        ----------
        transit_events: list[Connection]
            events are assumed to be ordered in DECREASING departure_time (!)
        target_stop: int
            index of the target stop
        start_time : int, optional
            start time in unixtime seconds
        end_time: int, optional
            end time in unixtime seconds (no connections will be scanned after this time)
        transfer_margin: int, optional
            required extra margin required for transfers in seconds
        walk_speed: float, optional
            walking speed between stops in meters / second.
        walk_network: networkx.Graph, optional
            each edge should have the walking distance as a data attribute ("distance_shape") expressed in meters
        verbose: boolean, optional
            whether to print out progress
        node_profile_class:
            NodeProfile.class
        """
        AbstractRoutingAlgorithm.__init__(self)

        self._target = target_stop
        self._connections = transit_events
        if start_time is None:
            start_time = transit_events[-1].departure_time
        if end_time is None:
            end_time = transit_events[0].departure_time
        self._start_time = start_time
        self._end_time = end_time
        self._transfer_margin = transfer_margin
        if walk_network is None:
            walk_network = networkx.Graph()
        self._walk_network = walk_network
        self._walk_speed = walk_speed
        self._verbose = verbose

        # algorithm internals

        # trip flags:
        self.__trip_min_arrival_time = defaultdict(lambda: float("inf"))

        # initialize stop_profiles
        self._stop_profiles = defaultdict(lambda: node_profile_class())
        self._stop_profiles[self._target] = IdentityNodeProfile()

    def _run(self):
        # if source node in s1:
        latest_dep_time = float("inf")
        connections = self._connections  # list[Connection]
        n_connections = len(connections)
        for i, connection in enumerate(connections):
            if self._verbose and i % 1000 == 0:
                print(i, "/", n_connections)
            assert(isinstance(connection, Connection))
            departure_time = connection.departure_time
            assert(departure_time <= latest_dep_time)
            latest_dep_time = departure_time
            arrival_time = connection.arrival_time
            departure_stop = connection.departure_stop
            arrival_stop = connection.arrival_stop
            trip_id = connection.trip_id

            arrival_profile = self._stop_profiles[arrival_stop]  # NodeProfile
            dep_stop_profile = self._stop_profiles[departure_stop]

            earliest_arrival_time_via_transfer = arrival_profile.get_earliest_arrival_time_at_target(
                arrival_time + self._transfer_margin
            )
            earliest_arrival_time_via_same_trip = self.__trip_min_arrival_time[trip_id]
            earliest_arrival_time_via_walking_to_target = arrival_time + self._get_walk_time_to_target(arrival_stop)

            min_arrival_time = min(earliest_arrival_time_via_same_trip,
                                   earliest_arrival_time_via_transfer,
                                   earliest_arrival_time_via_walking_to_target)
            if min_arrival_time == float("inf"):
                continue
            if earliest_arrival_time_via_same_trip > min_arrival_time:
                self.__trip_min_arrival_time[trip_id] = earliest_arrival_time_via_transfer

            pareto_tuple = ParetoTuple(departure_time, min_arrival_time)
            updated_dep_stop = dep_stop_profile.update_pareto_optimal_tuples(pareto_tuple)

            if updated_dep_stop:
                self._scan_footpaths_to_departure_stop(departure_stop, departure_time, min_arrival_time)

    def _scan_footpaths_to_departure_stop(self, connection_dep_stop, connection_dep_time, arrival_time_target):
        """ A helper method for scanning the footpaths. Updates self._stop_profiles accordingly"""
        for _, neighbor, data in self._walk_network.edges_iter(nbunch=[connection_dep_stop],
                                                                 data=True):
            d_walk = data['d_walk']
            neighbor_dep_time = connection_dep_time - d_walk / self._walk_speed
            pt = ParetoTuple(departure_time=neighbor_dep_time, arrival_time_target=arrival_time_target)
            self._stop_profiles[neighbor].update_pareto_optimal_tuples(pt)

    def _get_walk_time_to_target(self, arrival_stop):
        if self._walk_network.has_edge(arrival_stop, self._target):
            return self._walk_network[arrival_stop][self._target]["d_walk"] / self._walk_speed
        else:
            return float("inf")

    @property
    def stop_profiles(self):
        """
        Returns
        -------
        _stop_profiles : dict[int, AbstractNodeProfile]
            The pareto tuples necessary.
        """
        assert self._has_run
        return self._stop_profiles




