from collections import defaultdict

import networkx
import numpy

from gtfspy.routing.models import Connection
from gtfspy.routing.abstract_routing_algorithm import AbstractRoutingAlgorithm
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import merge_pareto_frontiers, LabelTimeWithBoardingsCount, LabelTime, compute_pareto_front, \
    LabelVehLegCount, LabelTimeBoardingsAndRoute
from gtfspy.util import timeit


class MultiObjectivePseudoCSAProfiler(AbstractRoutingAlgorithm):
    """
    Implementation of the profile connection scan algorithm presented in

    http://i11www.iti.uni-karlsruhe.de/extra/publications/dpsw-isftr-13.pdf
    """

    def __init__(self,
                 transit_events,
                 targets,
                 start_time=None,
                 end_time=None,
                 transfer_margin=0,
                 walk_network=None,
                 walk_speed=1.5,
                 verbose=False,
                 track_vehicle_legs=True,
                 track_time=True,
                 track_route=False):
        """
        Parameters
        ----------
        transit_events: list[Connection]
            events are assumed to be ordered in DECREASING departure_time (!)
        targets: int, list
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
            whether to consider the number of vehicle legs
        track_time: boolean, optional
            whether to consider time in the set of pareto_optimal
        """
        AbstractRoutingAlgorithm.__init__(self)
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

        # initialize stop_profiles
        self._count_vehicle_legs = track_vehicle_legs
        self._consider_time = track_time

        assert(track_time or track_vehicle_legs)
        if track_vehicle_legs:
            if track_time:
                if track_route:
                    self._label_class = LabelTimeBoardingsAndRoute
                else:
                    self._label_class = LabelTimeWithBoardingsCount
            else:
                self._label_class = LabelVehLegCount
        else:
            self._label_class = LabelTime
        self._stop_departure_times, self._stop_arrival_times = self.__compute_stop_dep_and_arrival_times()
        self._all_nodes = set.union(set(self._stop_departure_times.keys()),
                                    set(self._stop_arrival_times.keys()),
                                    set(self._walk_network.nodes()))

        self._pseudo_connections = self.__compute_pseudo_connections()
        self._add_pseudo_connection_departures_to_stop_departure_times()
        self._all_connections = self._pseudo_connections + self._transit_connections
        self._all_connections.sort(key=lambda connection: -connection.departure_time)
        self._augment_all_connections_with_arrival_stop_next_dep_time()
        if isinstance(targets, list):
            self._targets = targets
        else:
            self._targets = [targets]
        self.reset(self._targets)

    @timeit
    def _add_pseudo_connection_departures_to_stop_departure_times(self):
        self._stop_departure_times_with_pseudo_connections = dict(self._stop_departure_times)
        for node in self._all_nodes:
            if node not in self._stop_departure_times_with_pseudo_connections:
                self._stop_departure_times_with_pseudo_connections[node] = list()
        for key, value in self._stop_departure_times_with_pseudo_connections.items():
            self._stop_departure_times_with_pseudo_connections[key] = list(value)
        for pseudo_connection in self._pseudo_connections:
            assert(isinstance(pseudo_connection, Connection))
            self._stop_departure_times_with_pseudo_connections[pseudo_connection.departure_stop]\
                .append(pseudo_connection.departure_time)
        for stop, dep_times in self._stop_departure_times_with_pseudo_connections.items():
            self._stop_departure_times_with_pseudo_connections[stop] = numpy.array(list(sorted(set(dep_times))))


    @timeit
    def __initialize_node_profiles(self):
        self._stop_profiles = dict()
        for node in self._all_nodes:
            walk_duration_to_target = float('inf')
            closest_target = None
            if node in self._targets:
                walk_duration_to_target = 0
                closest_target = node
            else:
                for target in self._targets:
                    if self._walk_network.has_edge(target, node):
                        edge_data = self._walk_network.get_edge_data(target, node)
                        walk_duration = edge_data["d_walk"] / float(self._walk_speed)
                        if walk_duration_to_target > walk_duration:
                            walk_duration_to_target = walk_duration
                            closest_target = target

            self._stop_profiles[node] = NodeProfileMultiObjective(dep_times=self._stop_departure_times_with_pseudo_connections[node],
                                                                  label_class=self._label_class,
                                                                  walk_to_target_duration=walk_duration_to_target,
                                                                  transit_connection_dep_times=self._stop_departure_times[node],
                                                                  closest_target=closest_target,
                                                                  node_id=node)
    @timeit
    def __compute_stop_dep_and_arrival_times(self):
        stop_departure_times = defaultdict(lambda: list())
        stop_arrival_times = defaultdict(lambda: list())
        for connection in self._transit_connections:
            stop_arrival_times[connection.arrival_stop].append(connection.arrival_time)
            stop_departure_times[connection.departure_stop].append(connection.departure_time)
        for stop in stop_departure_times:
            stop_departure_times[stop] = numpy.array(sorted(list(set(stop_departure_times[stop]))))
        for stop in stop_arrival_times:
            stop_arrival_times[stop] = numpy.array(sorted(list(set(stop_arrival_times[stop]))))
        return stop_departure_times, stop_arrival_times


    @timeit
    def __compute_pseudo_connections(self):
        print("Started computing pseudoconnections")
        pseudo_connections = []
        # DiGraph makes things iterate both ways (!)
        for u, v, data in networkx.DiGraph(self._walk_network).edges(data=True):
            walk_duration = data["d_walk"] / float(self._walk_speed)
            total_walk_time_with_transfer = walk_duration + self._transfer_margin
            in_times = self._stop_arrival_times[u]
            out_times = self._stop_departure_times[v]
            j = 0
            n_in_times = len(in_times)
            n_out_times = len(out_times)
            if n_in_times == 0 or n_out_times == 0:
                continue
            i = 0
            while i < n_in_times and j < n_out_times:
                if in_times[i] + total_walk_time_with_transfer > out_times[j]:
                    j += 1  # -> need to increase out_time
                else:
                    # if next element still satisfies the wanted condition, go on and increase i!
                    while i + 1 < n_in_times and in_times[i + 1] + total_walk_time_with_transfer < out_times[j]:
                        i += 1
                    dep_time = in_times[i]
                    arr_time = out_times[j]
                    from_stop = u
                    to_stop = v
                    waiting_time = arr_time - dep_time - total_walk_time_with_transfer
                    assert(waiting_time >= 0)
                    pseudo = Connection(from_stop, to_stop, arr_time - walk_duration, arr_time,
                                        trip_id=None, is_walk=True)
                    pseudo_connections.append(pseudo)
                    i += 1
        print("Computed pseudoconnections")
        return pseudo_connections

    @timeit
    def _augment_all_connections_with_arrival_stop_next_dep_time(self):
        for connection in self._all_connections:
            assert(isinstance(connection, Connection))
            to_stop = connection.arrival_stop

            arr_stop_dep_times = self._stop_departure_times_with_pseudo_connections[to_stop]

            arr_stop_next_dep_time = float('inf')
            if len(arr_stop_dep_times) > 0:
                if connection.is_walk:
                    index = numpy.searchsorted(arr_stop_dep_times, connection.arrival_time)
                else:
                    index = numpy.searchsorted(arr_stop_dep_times, connection.arrival_time + self._transfer_margin)
                if 0 <= index < len(arr_stop_dep_times):
                    arr_stop_next_dep_time = arr_stop_dep_times[index]
            if connection.is_walk and not (arr_stop_next_dep_time < float('inf')):
                assert (arr_stop_next_dep_time < float('inf'))
            connection.arrival_stop_next_departure_time = arr_stop_next_dep_time

    def _get_modified_arrival_node_labels(self, connection):
        # get all different "accessible" / arrival times (Pareto-optimal sets)
        arrival_profile = self._stop_profiles[connection.arrival_stop]  # NodeProfileMultiObjective
        assert (isinstance(arrival_profile, NodeProfileMultiObjective))

        arrival_node_labels_orig = arrival_profile.evaluate(connection.arrival_stop_next_departure_time,
                                                            first_leg_can_be_walk=not connection.is_walk,
                                                            connection_arrival_time=connection.arrival_time)

        increment_vehicle_count = (self._count_vehicle_legs and not connection.is_walk)
        # TODO: (?) this copying / modification logic should be moved to the Label / ForwardJourney class ?
        arrival_node_labels_modified = self._copy_and_modify_labels(
            arrival_node_labels_orig,
            connection,
            increment_vehicle_count=increment_vehicle_count,
            first_leg_is_walk=connection.is_walk
        )
        if connection.is_walk:
            connection.is_walk = True
        arrival_node_labels_modified = compute_pareto_front(arrival_node_labels_modified)
        return arrival_node_labels_modified

    def _get_trip_labels(self, connection):
        # best labels from this current trip
        if not connection.is_walk:
            trip_labels = self._copy_and_modify_labels(self.__trip_labels[connection.trip_id],
                                                       connection,
                                                       increment_vehicle_count=False,
                                                       first_leg_is_walk=False)
        else:
            trip_labels = list()
        return trip_labels

    @timeit
    def _run(self):
        previous_departure_time = float("inf")
        n_connections_tot = len(self._all_connections)
        for i, connection in enumerate(self._all_connections):
            # basic checking + printing progress:
            if self._verbose and i % 1000 == 0:
                print("\r", i, "/", n_connections_tot, " : ", float(i) / n_connections_tot, end='', flush=True)
            assert (isinstance(connection, Connection))
            assert (connection.departure_time <= previous_departure_time)
            previous_departure_time = connection.departure_time

            # Get labels from the stop (possibly subject to buffer time)
            arrival_node_labels = self._get_modified_arrival_node_labels(connection)
            # This is for the labels staying "in the vehicle"
            trip_labels = self._get_trip_labels(connection)

            # Then, compute Pareto-frontier of these alternatives:
            all_pareto_optimal_labels = merge_pareto_frontiers(arrival_node_labels, trip_labels)

            # Update labels for this trip
            if not connection.is_walk:
                self.__trip_labels[connection.trip_id] = all_pareto_optimal_labels

            # Update labels for the departure stop profile (later: with the sets of pareto-optimal labels)
            self._stop_profiles[connection.departure_stop].update(all_pareto_optimal_labels,
                                                                  connection.departure_time)

            """
            if i == 10000:
                print()
                print(i)


                for bag in self._stop_profiles[connection.departure_stop]._label_bags:
                    for label in bag:
                        print(label)

                for stop, container in self._stop_profiles.items():
                    print(stop)
                    for bag in container._label_bags:
                        for label in bag:
                            print(label)
                        # prev_label = label.previous_label
                        # print(label)

                exit()
                """
        print("finalizing profiles!")
        self._finalize_profiles()

    def _finalize_profiles(self):
        """
        Deal with the first walks by joining profiles to other stops within walking distance.
        """
        for stop, stop_profile in self._stop_profiles.items():
            assert (isinstance(stop_profile, NodeProfileMultiObjective))
            neighbor_label_bags = []
            walk_durations_to_neighbors = []
            departure_arrival_stop_pairs = []
            if stop_profile.get_walk_to_target_duration() != 0 and stop in self._walk_network.node:
                neighbors = networkx.all_neighbors(self._walk_network, stop)
                for neighbor in neighbors:
                    neighbor_profile = self._stop_profiles[neighbor]
                    assert (isinstance(neighbor_profile, NodeProfileMultiObjective))
                    neighbor_label_bags.append(neighbor_profile.get_labels_for_real_connections())
                    walk_durations_to_neighbors.append(self._walk_network.get_edge_data(stop, neighbor)["d_walk"] /
                                                       self._walk_speed)
                    departure_arrival_stop_pairs.append((stop, neighbor))
            stop_profile.finalize(neighbor_label_bags, walk_durations_to_neighbors, departure_arrival_stop_pairs)

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

    def _copy_and_modify_labels(self, labels, connection, increment_vehicle_count=False, first_leg_is_walk=False):
        if self._label_class == LabelTimeBoardingsAndRoute:
            labels_copy = [label.get_label_with_connection_added(connection) for label in labels]
        else:
            labels_copy = [label.get_copy() for label in labels]

        for label in labels_copy:
            label.departure_time = connection.departure_time
            label.movement_duration += connection.arrival_time - connection.departure_time
            if increment_vehicle_count:
                label.n_boardings += 1
            label.first_leg_is_walk = first_leg_is_walk

        return labels_copy

    def reset(self, targets):
        if isinstance(targets, list):
            self._targets = targets
        else:
            self._targets = [targets]
        for target in targets:
            assert(target in self._all_nodes)
        self.__initialize_node_profiles()
        self.__trip_labels = defaultdict(lambda: list())
        self._has_run = False
