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

from gtfspy.routing.connection import Connection
from gtfspy.routing.label import LabelTime
from gtfspy.routing.node_profile_simple import NodeProfileSimple
from gtfspy.routing.abstract_routing_algorithm import AbstractRoutingAlgorithm
from gtfspy.routing.pseudo_connections import compute_pseudo_connections
from gtfspy.routing.node_profile_c import NodeProfileC
from gtfspy.util import graph_has_node

class PseudoConnectionScanProfiler(AbstractRoutingAlgorithm):
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
                 verbose=False):
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
        """
        AbstractRoutingAlgorithm.__init__(self)

        self._target = target_stop
        self._transit_connections = transit_events
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
        self._walk_speed = float(walk_speed)
        self._verbose = verbose

        # algorithm internals

        # trip flags:
        self.__trip_min_arrival_time = defaultdict(lambda: float("inf"))

        # initialize stop_profiles
        self._stop_profiles = defaultdict(lambda: NodeProfileC())
        # initialize stop_profiles for target stop, and its neighbors
        self._stop_profiles[self._target] = NodeProfileC(0)
        if graph_has_node(walk_network, target_stop):
            for target_neighbor in walk_network.neighbors(target_stop):
                edge_data = walk_network.get_edge_data(target_neighbor, target_stop)
                walk_duration = edge_data["d_walk"] / self._walk_speed
                self._stop_profiles[target_neighbor] = NodeProfileC(walk_duration)
        pseudo_connection_set = compute_pseudo_connections(transit_events, self._start_time, self._end_time,
                                                           self._transfer_margin, self._walk_network,
                                                           self._walk_speed)
        self._pseudo_connections = list(pseudo_connection_set)
        self._all_connections = self._pseudo_connections + self._transit_connections
        self._all_connections.sort(key=lambda connection: -connection.departure_time)

    def _run(self):
        # if source node in s1:
        previous_departure_time = float("inf")
        connections = self._all_connections # list[Connection]
        n_connections_tot = len(connections)
        for i, connection in enumerate(connections):
            # basic checking + printing progress:
            if self._verbose and i % 1000 == 0:
                print(i, "/", n_connections_tot)
            assert (isinstance(connection, Connection))
            assert (connection.departure_time <= previous_departure_time)
            previous_departure_time = connection.departure_time

            # get all different "accessible" / arrival times (Pareto-optimal sets)
            arrival_profile = self._stop_profiles[connection.arrival_stop]  # NodeProfileSimple

            # Three possibilities:

            # 1. earliest arrival time (Profiles) via transfer
            earliest_arrival_time_via_transfer = arrival_profile.evaluate_earliest_arrival_time_at_target(
                connection.arrival_time, self._transfer_margin
            )

            # 2. earliest arrival time within same trip (equals float('inf') if not reachable)
            earliest_arrival_time_via_same_trip = self.__trip_min_arrival_time[connection.trip_id]

            # then, take the minimum (or the Pareto-optimal set) of these three alternatives.
            min_arrival_time = min(earliest_arrival_time_via_same_trip,
                                   earliest_arrival_time_via_transfer)

            # If there are no 'labels' to progress, nothing needs to be done.
            if min_arrival_time == float("inf"):
                continue

            # Update information for the trip
            if (not connection.is_walk) and (earliest_arrival_time_via_same_trip > min_arrival_time):
                self.__trip_min_arrival_time[connection.trip_id] = earliest_arrival_time_via_transfer

            # Compute the new "best" pareto_tuple possible (later: merge the sets of pareto-optimal labels)
            pareto_tuple = LabelTime(connection.departure_time, min_arrival_time)

            # update departure stop profile (later: with the sets of pareto-optimal labels)
            self._stop_profiles[connection.departure_stop].update_pareto_optimal_tuples(pareto_tuple)

    @property
    def stop_profiles(self):
        """
        Returns
        -------
        _stop_profiles : dict[int, NodeProfileSimple]
            The pareto tuples necessary.
        """
        assert self._has_run
        return self._stop_profiles
