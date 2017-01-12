from unittest import TestCase

import networkx

from gtfspy.routing.models import Connection
from gtfspy.routing.label import min_arrival_time_target, LabelTimeWithBoardingsCount, LabelTime
from gtfspy.routing.multi_objective_pseudo_connection_scan_profiler import MultiObjectivePseudoCSAProfiler
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective

import pyximport
pyximport.install()


class TestMultiObjectivePseudoCSAProfiler(TestCase):
    # noinspection PyAttributeOutsideInit

    def setUp(self):
        event_list_raw_data = [
            (2, 4, 40, 50, "trip_6"),
            (1, 3, 32, 40, "trip_5"),
            (3, 4, 32, 35, "trip_4"),
            (2, 3, 25, 30, "trip_3"),
            (1, 2, 10, 20, "trip_2"),
            (0, 1, 0, 10, "trip_1")
        ]
        self.transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        self.walk_network = networkx.Graph()
        self.walk_network.add_edge(1, 2, {"d_walk": 20})
        self.walk_network.add_edge(3, 4, {"d_walk": 15})
        self.walk_speed = 1
        self.target_stop = 4
        self.transfer_margin = 0
        self.start_time = 0
        self.end_time = 50


    def test_pseudo_connections(self):
        event_list_raw_data = [
            (0, 1, 10, 20, "trip_6"),
            (2, 3, 42, 50, "trip_5")
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, {"d_walk": 20})
        walk_speed = 1
        target_stop = 3
        transfer_margin = 0
        start_time = 0
        end_time = 50
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        self.assertEqual(len(csa_profile._all_connections), 3)
        pseudo_connection = csa_profile._all_connections[1]
        self.assertTrue(pseudo_connection.is_walk)
        self.assertEqual(pseudo_connection.waiting_time, 2)
        self.assertEqual(pseudo_connection.departure_time, 20)
        self.assertEqual(pseudo_connection.arrival_time, 42)
        self.assertEqual(pseudo_connection.departure_stop, 1)
        self.assertEqual(pseudo_connection.arrival_stop, 2)

        node_to_connection_dep_times = {
            0: [10],
            1: [20],
            2: [42],
            3: [],
        }
        for node, dep_times in node_to_connection_dep_times.items():
            profile = csa_profile._stop_profiles[node]
            for dep_time in dep_times:
                self.assertIn(dep_time, profile.dep_times_to_index, "Node: " + str(node))
            for dep_time in profile.dep_times_to_index:
                self.assertIn(dep_time, dep_times, "Node: " + str(node))

        for connection in csa_profile._all_connections:
            arrival_stop_profile = csa_profile._stop_profiles[connection.arrival_stop]
            departure_stop_profile = csa_profile._stop_profiles[connection.departure_stop]
            self.assertIsInstance(arrival_stop_profile, NodeProfileMultiObjective)
            self.assertIsInstance(departure_stop_profile, NodeProfileMultiObjective)
            self.assertIn(connection.departure_time, departure_stop_profile.dep_times_to_index)
            if connection.arrival_stop_next_departure_time != float('inf'):
                self.assertIn(connection.arrival_stop_next_departure_time, arrival_stop_profile.dep_times_to_index)

    def test_basics(self):
        csa_profile = MultiObjectivePseudoCSAProfiler(self.transit_connections, self.target_stop,
                                                      self.start_time, self.end_time, self.transfer_margin,
                                                      self.walk_network, self.walk_speed)
        csa_profile.run()

        stop_3_labels = csa_profile.stop_profiles[3].get_final_optimal_labels()
        self.assertEqual(len(stop_3_labels), 1)
        self.assertIn(LabelTimeWithBoardingsCount(32, 35, n_boardings=1, first_leg_is_walk=False), stop_3_labels)

        stop_2_labels = csa_profile.stop_profiles[2].get_final_optimal_labels()
        self.assertEqual(len(stop_2_labels), 3)
        self.assertIn(LabelTimeWithBoardingsCount(40, 50, n_boardings=1, first_leg_is_walk=False), stop_2_labels)
        self.assertIn(LabelTimeWithBoardingsCount(25, 35, n_boardings=2, first_leg_is_walk=False), stop_2_labels)
        self.assertIn(LabelTimeWithBoardingsCount(25, 45, n_boardings=1, first_leg_is_walk=False), stop_2_labels)


        stop_one_profile = csa_profile.stop_profiles[1]
        stop_one_pareto_labels = stop_one_profile.get_final_optimal_labels()

        labels = list()
        # these should exist at least:
        labels.append(LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=35, n_boardings=3, first_leg_is_walk=False))
        labels.append(LabelTimeWithBoardingsCount(departure_time=20, arrival_time_target=50, n_boardings=1, first_leg_is_walk=False))
        labels.append(LabelTimeWithBoardingsCount(departure_time=32, arrival_time_target=55, n_boardings=1, first_leg_is_walk=False))

    def test_multiple_targets(self):
        event_list_raw_data = [
            (1, 4, 40, 50, "trip"),
            (1, 5, 30, 40, "trip"),
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_speed = 1
        source_stop = 1
        targets = [4, 5]
        transfer_margin = 0
        start_time = 0
        end_time = 60

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, targets,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        source_stop_profile = csa_profile.stop_profiles[source_stop]
        final_labels = source_stop_profile.get_final_optimal_labels()
        self.assertEqual(2, len(final_labels))

    def test_simple(self):
        event_list_raw_data = [
            (2, 4, 40, 50, "trip_5"),
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, {"d_walk": 20})
        walk_network.add_edge(3, 4, {"d_walk": 15})
        walk_speed = 1
        source_stop = 1
        target_stop = 4
        transfer_margin = 0
        start_time = 0
        end_time = 50

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        source_stop_profile = csa_profile.stop_profiles[source_stop]
        self.assertTrue(source_stop_profile._finalized)
        self.assertTrue(source_stop_profile._closed)

        source_stop_labels = source_stop_profile.get_final_optimal_labels()

        labels = list()
        labels.append(LabelTimeWithBoardingsCount(departure_time=20, arrival_time_target=50,
                                                  n_boardings=1, first_leg_is_walk=True))

        self._assert_label_sets_equal(
            labels,
            source_stop_labels
        )

    def test_last_leg_is_walk(self):
        event_list_raw_data = [
            (0, 1, 0, 10, "trip_1")
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, {"d_walk": 20})

        walk_speed = 1
        source_stop = 0
        target_stop = 2
        transfer_margin = 0
        start_time = 0
        end_time = 50
        labels = list()
        labels.append(LabelTimeWithBoardingsCount(departure_time=0, arrival_time_target=30, n_boardings=1, first_leg_is_walk=False))

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        found_tuples = csa_profile.stop_profiles[source_stop].get_final_optimal_labels()
        self._assert_label_sets_equal(found_tuples, labels)

    def test_walk_is_faster_than_by_trip(self):
        event_list_raw_data = [
            (0, 1, 0, 10, "trip_1")
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_speed = 2
        source_stop = 0
        target_stop = 1
        transfer_margin = 0
        start_time = 0
        end_time = 50

        walk_network = networkx.Graph()
        walk_network.add_edge(0, 1, {"d_walk": 1})
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        source_profile = csa_profile.stop_profiles[source_stop]
        self.assertEqual(min_arrival_time_target(source_profile.evaluate(0, first_leg_can_be_walk=True)), 0.5)
        found_tuples = source_profile.get_final_optimal_labels()
        self.assertEqual(len(found_tuples), 0)

    def test_no_multiple_walks(self):
        event_list_raw_data = [
            (0, 1, 0, 1, "trip_1"),
            (1, 0, 0, 1, "trip_2"),
            (0, 1, 2, 3, "trip_3"),
            (1, 0, 2, 3, "trip_4"),
            (0, 1, 4, 5, "trip_5"),
            (1, 0, 4, 5, "trip_6"),
            (1, 2, 5, 6, "trip_7"),
            (2, 1, 5, 6, "trip_8"),
            (1, 2, 2, 3, "trip_7"),
            (2, 1, 2, 3, "trip_8")
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(0, 1, {"d_walk": 1})
        walk_network.add_edge(2, 1, {"d_walk": 1})
        walk_speed = 10
        transfer_margin = 0
        start_time = 0
        end_time = 50

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, 2,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        source_profile = csa_profile.stop_profiles[0]
        print(source_profile.get_final_optimal_labels())
        for label in source_profile.get_final_optimal_labels():
            self.assertGreater(label.n_boardings, 0)

    def test_target_node_not_in_walk_network(self):
        event_list_raw_data = [
            (0, 1, 0, 10, "trip_1")
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_speed = 2
        source_stop = 0
        target_stop = 1
        transfer_margin = 0
        start_time = 0
        end_time = 50

        walk_network = networkx.Graph()
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        source_profile = csa_profile.stop_profiles[source_stop]
        self.assertEqual(min_arrival_time_target(source_profile.evaluate(0, 0)), 10)
        found_tuples = source_profile.get_final_optimal_labels()
        self.assertEqual(len(found_tuples), 1)

    def test_pareto_optimality(self):
        event_list_raw_data = [
            (0, 2, 0, 10, "trip_1"),
            (0, 1, 2, 5, "trip_2"),
            (1, 2, 5, 8, "trip_3")
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_speed = 2
        source_stop = 0
        target_stop = 2
        transfer_margin = 0
        start_time = 0
        end_time = 20
        walk_network = networkx.Graph()
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        source_profile = csa_profile.stop_profiles[source_stop]
        self.assertEqual(min_arrival_time_target(source_profile.evaluate(0, 0)), 8)
        found_labels = source_profile.get_final_optimal_labels()
        labels_should_be = list()
        labels_should_be.append(LabelTimeWithBoardingsCount(0, 10, n_boardings=1, first_leg_is_walk=False))
        labels_should_be.append(LabelTimeWithBoardingsCount(2, 8, n_boardings=2, first_leg_is_walk=False))
        self._assert_label_sets_equal(found_labels, labels_should_be)

    def test_transfer_margin(self):
        walk_speed = 1
        target_stop = 2
        start_time = 0
        end_time = 60
        transit_connections = [
            Connection(0, 1, 40, 50, "trip_1"),
            Connection(1, 2, 50, 60, "trip_1"),
            Connection(3, 1, 40, 50, "trip_2"),
        ]
        # case without any transfer margin
        transfer_margin = 0
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      networkx.Graph(), walk_speed)
        csa_profile.run()
        stop_profile_1 = csa_profile.stop_profiles[1]
        stop_profile_3 = csa_profile.stop_profiles[3]
        self.assertEqual(1, len(stop_profile_1.get_final_optimal_labels()))
        self.assertEqual(1, len(stop_profile_3.get_final_optimal_labels()))

        # case with transfer margin
        transfer_margin = 1
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      networkx.Graph(), walk_speed)
        csa_profile.run()
        stop_profile_3 = csa_profile.stop_profiles[3]
        stop_profile_1 = csa_profile.stop_profiles[1]
        self.assertEqual(0, len(stop_profile_3.get_final_optimal_labels()))
        self.assertEqual(1, len(stop_profile_1.get_final_optimal_labels()))

    def test_basics_no_transfer_tracking(self):
        csa_profile = MultiObjectivePseudoCSAProfiler(
            self.transit_connections, self.target_stop,
            self.start_time, self.end_time, self.transfer_margin,
            self.walk_network, self.walk_speed, track_vehicle_legs=False
        )
        csa_profile.run()

        stop_3_pareto_tuples = csa_profile.stop_profiles[3].get_final_optimal_labels()
        self.assertEqual(len(stop_3_pareto_tuples), 1)
        self.assertIn(LabelTime(32, 35), stop_3_pareto_tuples)

        stop_2_pareto_tuples = csa_profile.stop_profiles[2].get_final_optimal_labels()
        self.assertEqual(len(stop_2_pareto_tuples), 2)
        self.assertIn(LabelTime(40, 50), stop_2_pareto_tuples)
        self.assertIn(LabelTime(25, 35), stop_2_pareto_tuples)

        source_stop_profile = csa_profile.stop_profiles[1]
        source_stop_pareto_optimal_tuples = source_stop_profile.get_final_optimal_labels()

        pareto_tuples = list()
        pareto_tuples.append(LabelTime(departure_time=10, arrival_time_target=35))
        pareto_tuples.append(LabelTime(departure_time=20, arrival_time_target=50))
        pareto_tuples.append(LabelTime(departure_time=32, arrival_time_target=55))

        self._assert_label_sets_equal(
            pareto_tuples,
            source_stop_pareto_optimal_tuples
        )

    def test_transfers_only(self):
        event_list_raw_data = [
            (7, 2, 20, 30, "trip_6"),
            (2, 4, 40, 50, "trip_5"),
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, {"d_walk": 20})
        walk_network.add_edge(3, 4, {"d_walk": 15})
        walk_speed = 1
        target_stop = 4
        transfer_margin = 0
        start_time = 0
        end_time = 50

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed, track_time=False)
        csa_profile.run()

        stop_to_n_boardings = {
            2: 1,
            7: 2,
            3: 0
        }

        for stop, n_veh_legs in stop_to_n_boardings.items():
            labels = csa_profile.stop_profiles[stop].get_final_optimal_labels()
            self.assertEqual(len(labels), 1)
            self.assertEqual(labels[0].n_boardings, n_veh_legs)

    def test_reset(self):
        walk_speed = 1
        target_stop = 2
        start_time = 0
        end_time = 60
        transfer_margin = 0
        transit_connections = [
            Connection(0, 1, 40, 50, "trip_1"),
            Connection(1, 2, 55, 60, "trip_1"),
            Connection(3, 1, 40, 60, "trip_2"),
        ]
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      networkx.Graph(), walk_speed)
        csa_profile.run()
        nodes = [0, 1, 2, 3]
        label_counts = [1, 1, 0, 0]
        for node, count in zip(nodes, label_counts):
            n_labels = len(csa_profile.stop_profiles[node].get_final_optimal_labels())
            self.assertEqual(n_labels, count)

        target_stops = [1]
        csa_profile.reset(target_stops)
        csa_profile.run()
        label_counts = [1, 0, 0, 1]
        for node, count in zip(nodes, label_counts):
            n_labels = len(csa_profile.stop_profiles[node].get_final_optimal_labels())
            self.assertEqual(n_labels, count)

        # TODO: perform a check for the reinitialization of trip_labels
        # THIS IS NOT YET TESTED but should work at the moment
        # RK 9.1.2017


    def _assert_label_sets_equal(self, found_tuples, should_be_tuples):
        self.assertEqual(len(found_tuples), len(should_be_tuples))
        for found_tuple in found_tuples:
            self.assertIn(found_tuple, should_be_tuples)
        for should_be_tuple in should_be_tuples:
            self.assertIn(should_be_tuple, found_tuples)
