from collections import defaultdict

import networkx

from gtfspy.routing.models import Connection
from gtfspy.routing.abstract_routing_algorithm import AbstractRoutingAlgorithm
from gtfspy.routing.pseudo_connections import compute_pseudo_connections
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import merge_pareto_frontiers, LabelTimeAndVehLegCount, LabelTime, compute_pareto_front, \
    LabelVehLegCount


class MultiObjectivePseudoCSAProfiler(AbstractRoutingAlgorithm):
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
                 track_vehicle_legs=True,
                 track_time=True):
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
        track_vehicle_legs: boolean, optional
            whether to consider the nubmer of vehicle legs
        track_time: boolean, optional
            whether to consider time in the set of pareto_optimal
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
        self._walk_speed = walk_speed
        self._verbose = verbose

        # algorithm internals

        # trip flags:
        self.__trip_labels = defaultdict(lambda: set())

        # initialize stop_profiles
        self._count_vehicle_legs = track_vehicle_legs
        self._consider_time = track_time

        assert(track_time or track_vehicle_legs)
        if track_vehicle_legs:
            if track_time:
                self._label_class = LabelTimeAndVehLegCount
            else:
                self._label_class = LabelVehLegCount
        else:
            self._label_class = LabelTime

        self._stop_profiles = defaultdict(lambda: NodeProfileMultiObjective(label_class=self._label_class))
        # initialize stop_profiles for target stop, and its neighbors
        self._stop_profiles[self._target] = NodeProfileMultiObjective(0, label_class=self._label_class)
        if target_stop in walk_network.nodes():
            for target_neighbor in walk_network.neighbors(target_stop):
                edge_data = walk_network.get_edge_data(target_neighbor, target_stop)
                walk_duration = edge_data["d_walk"] / self._walk_speed
                self._stop_profiles[target_neighbor] = NodeProfileMultiObjective(walk_duration,
                                                                                 label_class=self._label_class)
        self._compute_pseudo_connections()

    def _compute_pseudo_connections(self):
        print("Started computing pseudoconnections")
        pseudo_connection_set = compute_pseudo_connections(self._transit_connections, self._start_time, self._end_time,
                                                           self._transfer_margin, self._walk_network,
                                                           self._walk_speed)
        self._pseudo_connections = list(pseudo_connection_set)
        self._all_connections = self._pseudo_connections + self._transit_connections
        self._all_connections.sort(key=lambda connection: -connection.departure_time)
        print("Computed pseudoconnections")

    def _run(self):
        # if source node in s1:
        previous_departure_time = float("inf")
        n_connections_tot = len(self._all_connections)
        for i, connection in enumerate(self._all_connections):
            # basic checking + printing progress:
            if self._verbose and i % 1000 == 0:
                print(i, "/", n_connections_tot)
            assert (isinstance(connection, Connection))
            assert (connection.departure_time <= previous_departure_time)
            previous_departure_time = connection.departure_time

            # get all different "accessible" / arrival times (Pareto-optimal sets)
            arrival_profile = self._stop_profiles[connection.arrival_stop]  # NodeProfileMultiObjective
            assert(isinstance(arrival_profile, NodeProfileMultiObjective))

            # Two possibilities:

            # "best labels at the arrival node", double walks are not allowed

            arrival_node_labels_orig = arrival_profile.evaluate(connection.arrival_time, self._transfer_margin,
                                                                allow_walk_to_target=not connection.is_walk)

            increment_vehicle_count = (self._count_vehicle_legs and not connection.is_walk)
            arrival_node_labels = self._copy_and_modify_labels(arrival_node_labels_orig,
                                                               connection.departure_time,
                                                               increment_vehicle_count=increment_vehicle_count)
            arrival_node_labels = set(compute_pareto_front(list(arrival_node_labels)))

            # best labels from this current trip
            if not connection.is_walk:
                trip_labels = self._copy_and_modify_labels(self.__trip_labels[connection.trip_id],
                                                           connection.departure_time,
                                                           increment_vehicle_count=False)
            else:
                trip_labels = set()

            # then, take the Pareto-optimal set of these alternatives:
            all_pareto_optimal_labels = merge_pareto_frontiers(arrival_node_labels, trip_labels)

            # Update information for the trip
            if not connection.is_walk:
                self.__trip_labels[connection.trip_id] = all_pareto_optimal_labels

            # update departure stop profile (later: with the sets of pareto-optimal labels)
            self._stop_profiles[connection.departure_stop].update(all_pareto_optimal_labels)

    @property
    def stop_profiles(self):
        """
        Returns
        -------
        _stop_profiles : dict[int, NodeProfileMultiObjective]
            The pareto tuples necessary.
        """
        assert self._has_run
        return self._stop_profiles

    def _copy_and_modify_labels(self, labels, departure_time, increment_vehicle_count=False):
        labels_copy = [label.get_copy() for label in labels]
        for label in labels_copy:
            label.departure_time = departure_time
            if increment_vehicle_count:
                label.n_vehicle_legs += 1
        return labels_copy
