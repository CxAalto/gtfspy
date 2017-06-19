from unittest import TestCase

import networkx
from six import StringIO

from gtfspy.routing.connection import Connection
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
        self.assertEqual(pseudo_connection.departure_time, 42 - 20)
        self.assertEqual(pseudo_connection.arrival_time, 42)
        self.assertEqual(pseudo_connection.departure_stop, 1)
        self.assertEqual(pseudo_connection.arrival_stop, 2)

        node_to_connection_dep_times = {
            0: [10],
            1: [42 - 20],
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


    def test_pseudo_connections_with_transfer_margin(self):
        event_list_raw_data = [
            (0, 1, 10, 20, "trip_6"),
            (2, 3, 42, 50, "trip_5")
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, {"d_walk": 10})
        walk_speed = 1
        target_stop = 3
        transfer_margin = 5
        start_time = 0
        end_time = 50
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        transfer_connection = csa_profile._all_connections[1]
        self.assertEqual(transfer_connection.arrival_stop, 2)
        self.assertEqual(transfer_connection.arrival_stop_next_departure_time, 42)
        self.assertEqual(transfer_connection.departure_stop, 1)
        self.assertEqual(transfer_connection.departure_time, 42 - 10)
        self.assertEqual(transfer_connection.is_walk, True)
        self.assertEqual(transfer_connection.arrival_time, 42)

        print(transfer_connection)

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

    def test_possible_transfer_margin_bug_with_multiple_arrivals(self):
        walk_speed = 1
        target_stop = 3
        start_time = 0
        end_time = 200
        transfer_margin = 2
        transit_connections = [
            Connection(0, 1, 100, 101, "trip_0"),
            Connection(4, 1, 102, 104, "trip_1"),
            Connection(2, 3, 106, 108, "trip_2")
        ]
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, {"d_walk": 1})
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        profile = csa_profile.stop_profiles[4]
        self.assertEqual(len(profile.get_final_optimal_labels()), 0)
        profile = csa_profile.stop_profiles[0]
        self.assertEqual(len(profile.get_final_optimal_labels()), 1)

    def test_transfer_margin_with_walk(self):
        walk_speed = 1
        target_stop = 3
        start_time = 0
        end_time = 200
        transit_connections = [
            Connection(0, 1, 100, 101, "trip__2"),
            Connection(0, 1, 101, 102, "trip__1"),
            Connection(0, 1, 102, 103, "trip_0"),
            Connection(0, 1, 100, 101, "trip_1"),
            Connection(0, 1, 101, 102, "trip_2"),
            Connection(0, 1, 102, 103, "trip_3"),
            Connection(0, 1, 103, 104, "trip_4"),
            Connection(2, 3, 106, 107, "trip_6"),
        ]

        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, {"d_walk": 0.5})
        transfer_margins = [1, 2, 3, 4, 0]
        journey_dep_times = [103, 102, 101, 100, 103]

        for transfer_margin, dep_time in zip(transfer_margins, journey_dep_times):
            csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                          start_time, end_time, transfer_margin,
                                                          walk_network, walk_speed)
            csa_profile.run()
            profile = csa_profile.stop_profiles[0]
            self.assertEqual(len(profile.get_final_optimal_labels()), 1, "transfer_margin=" + str(transfer_margin))
            label = profile.get_final_optimal_labels()[0]
            self.assertEqual(label.departure_time, dep_time, "transfer_margin=" + str(transfer_margin))

    def test_basics_no_transfer_tracking(self):
        csa_profile = MultiObjectivePseudoCSAProfiler(
            self.transit_connections, self.target_stop,
            self.start_time, self.end_time, self.transfer_margin,
            self.walk_network, self.walk_speed, track_vehicle_legs=False
        )
        csa_profile.run()

        stop_3_pareto_tuples = csa_profile.stop_profiles[3].get_final_optimal_labels()
        self.assertEqual(len(stop_3_pareto_tuples), 1)
        self.assertIn(LabelTime(32., 35.), stop_3_pareto_tuples)

        stop_2_pareto_tuples = csa_profile.stop_profiles[2].get_final_optimal_labels()
        self.assertEqual(len(stop_2_pareto_tuples), 2)
        self.assertIn(LabelTime(40., 50.), stop_2_pareto_tuples)
        self.assertIn(LabelTime(25., 35.), stop_2_pareto_tuples)

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

    def test_550_problem(self):
        # There used to be a problem when working with real unixtimes (c-side floating point number problems),
        # this test is one check for that
        event_data = StringIO(
            "from_stop_I,to_stop_I,dep_time_ut,arr_time_ut,route_type,route_id,trip_I,seq\n" +
            "2198,2247,1475530740,1475530860,3,2550,158249,36\n" +
            "2247,2177,1475530860,1475530980,3,2550,158249,37\n")
        import pandas as pd
        events = pd.read_csv(event_data)
        events.sort_values("dep_time_ut", ascending=False, inplace=True)
        connections = [
            Connection(int(e.from_stop_I), int(e.to_stop_I), int(e.dep_time_ut), int(e.arr_time_ut), int(e.trip_I))
            for e in events.itertuples()
        ]
        csa_profiler = MultiObjectivePseudoCSAProfiler(connections, 2177,
                                                      0, 1475530860*10, 0,
                                                      networkx.Graph(), 0)

        csa_profiler.run()

        profiles = csa_profiler.stop_profiles
        labels_2198 = profiles[2198].get_final_optimal_labels()
        self.assertEqual(len(labels_2198), 1)
        self.assertEqual(labels_2198[0].duration(), 1475530980 - 1475530740)
        labels_2247 = profiles[2247].get_final_optimal_labels()
        self.assertEqual(len(labels_2247), 1)
        self.assertEqual(labels_2247[0].duration(), 1475530980 - 1475530860)

    def test_transfer_on_same_stop_with_multiple_departures(self):
        walk_speed = 1000
        target_stop = 5
        start_time = 0
        end_time = 60
        transfer_margin = 0
        transit_connections = [
            Connection(0, 4, 30, 40, "trip_1"),
            Connection(4, 1, 50, 60, "trip_2"),
            Connection(4, 2, 50, 60, "trip_3"),
            Connection(4, 3, 50, 60, "trip_4"),
            Connection(4, target_stop, 70, 100, "trip_5")
        ]
        csa_profiler = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      networkx.Graph(), walk_speed)
        csa_profiler.run()
        profiles = csa_profiler.stop_profiles
        assert(profiles[0].get_final_optimal_labels()[0])
        assert(len(profiles[0].get_final_optimal_labels()) > 0)

    def test_transfer_connections_do_not_affect_transfers(self):
        walk_speed = 1000
        target_stop = 1233412
        start_time = 0
        end_time = 60
        transfer_margin = 0
        transit_connections = [
            Connection(0, 1, 30, 40, "trip_1"),
            Connection(3, 4, 45, 50, "trip_2"),
            Connection(4, 3, 45, 50, "trip_3"),
            Connection(5, 3, 45, 50, "trip_4"),
            Connection(1, target_stop, 70, 100, "trip_5")
        ]
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 3, {"d_walk": 1})
        walk_network.add_edge(1, 4, {"d_walk": 1})
        walk_network.add_edge(1, 5, {"d_walk": 1})
        csa_profiler = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                       start_time, end_time, transfer_margin,
                                                       walk_network, walk_speed)
        csa_profiler.run()
        profiles = csa_profiler.stop_profiles
        assert(profiles[0].get_final_optimal_labels()[0])
        assert(len(profiles[0].get_final_optimal_labels()) > 0)


    def test_transfer_connections_do_not_affect_transfers2(self):
        walk_speed = 1
        target_stop = 0
        start_time = 0
        end_time = 60
        transfer_margin = 0
        transit_connections = [
            Connection(3, 0, 10, 11, "trip_1"),
            Connection(2, 1, 5, 6, "trip_2"),
            Connection(4, 3, 0, 1, "trip_3")
        ]
        walk_network = networkx.Graph()
        walk_network.add_edge(2, 3, {"d_walk": 1})
        walk_network.add_edge(1, 0, {"d_walk": 1})
        csa_profiler = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                       start_time, end_time, transfer_margin,
                                                       walk_network, walk_speed)
        csa_profiler.run()
        profiles = csa_profiler.stop_profiles
        assert(len(profiles[4].get_final_optimal_labels()) == 1)
        optimal_label = profiles[4].get_final_optimal_labels()[0]
        self.assertEqual(optimal_label.departure_time, 0)
        self.assertEqual(optimal_label.arrival_time_target, 7)
        self.assertEqual(optimal_label.n_boardings, 2)

    def test_transfer_connections_do_not_affect_transfers3(self):
        walk_speed = 1
        target_stop = 0
        start_time = 0
        end_time = 60
        transfer_margin = 0
        transit_connections = [
            Connection(3, 0, 10, 11, "t1"),
            Connection(2, 1, 5, 6, "t2"),
            Connection(7, 2, 3, 4, "tX"),
            Connection(5, 6, 2, 3, "--"),
            Connection(4, 3, 0, 1, "t3")
        ]

        walk_network = networkx.Graph()
        walk_network.add_edge(7, 3, {"d_walk": 1})
        walk_network.add_edge(1, 0, {"d_walk": 1})
        walk_network.add_edge(5, 3, {"d_walk": 1})
        csa_profiler = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                       start_time, end_time, transfer_margin,
                                                       walk_network, walk_speed)
        csa_profiler.run()
        profiles = csa_profiler.stop_profiles
        print(profiles[4].get_final_optimal_labels()[0])
        optimal_labels = profiles[4].get_final_optimal_labels()
        assert(len(optimal_labels) == 2)
        boardings_to_arr_time = {}
        for label in optimal_labels:
            boardings_to_arr_time[label.n_boardings] = label.arrival_time_target
        self.assertEqual(boardings_to_arr_time[2], 11)
        self.assertEqual(boardings_to_arr_time[3], 7)

    def _assert_label_sets_equal(self, found_tuples, should_be_tuples):
        self.assertEqual(len(found_tuples), len(should_be_tuples))
        for found_tuple in found_tuples:
            self.assertIn(found_tuple, should_be_tuples)
        for should_be_tuple in should_be_tuples:
            self.assertIn(should_be_tuple, found_tuples)

    def test_stored_route(self):
        # TODO:
        # - test with multiple targets
        # - test with continuing route
        # - test that timestamps for label and the connection objects match
        csa_profile = MultiObjectivePseudoCSAProfiler(self.transit_connections, self.target_stop,
                                                      self.start_time, self.end_time, self.transfer_margin,
                                                      self.walk_network, self.walk_speed, track_route=True)
        csa_profile.run()
        for stop, profile in csa_profile.stop_profiles.items():
            for bag in profile._label_bags:
                for label in bag:
                    # print(stop, label)
                    cur_label = label
                    journey_legs = []
                    while True:
                        connection = cur_label.connection
                        if isinstance(connection, Connection):
                            journey_legs.append(connection)
                        if not cur_label.previous_label:
                            break
                        cur_label = cur_label.previous_label
                    route_tuples_list = [(x.departure_stop, x.arrival_stop) for x in journey_legs]
                    # print(route_tuples_list)
                    # test that all legs are unique
                    self.assertEqual(len(route_tuples_list), len(set(route_tuples_list)))
                    prev_arr_node = None
                    for route_tuple in route_tuples_list:
                        dep_node = route_tuple[0]
                        arr_node = route_tuple[1]
                        # test that all legs have unique departure and arrival nodes
                        self.assertNotEqual(dep_node, arr_node)
                        if prev_arr_node:
                            # test that legs form an continuous path
                            self.assertEqual(prev_arr_node, dep_node)
                        prev_arr_node = arr_node

    def test_target_self_loops(self):
        event_list_raw_data = [
            (3, 1, 30, 40, "trip_3"),

        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 3, {"d_walk": 11})
        walk_speed = 1
        target_stop = 1
        transfer_margin = 0
        start_time = 0
        end_time = 50
        print(walk_network.edges())
        print(transit_connections)
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed, track_vehicle_legs=True,
                                                      track_time=True, track_route=True)
        csa_profile.run()
        for stop, profile in csa_profile.stop_profiles.items():
            if stop == target_stop:
                self.assertEqual(len(profile.get_final_optimal_labels()), 0)

    def test_journeys_using_movement_duration(self):
        def unpack_route_from_labels(cur_label):
            route = []
            last_arrival_stop = None
            while True:
                connection = cur_label.connection
                if isinstance(connection, Connection):
                    route.append(connection.departure_stop)

                if not cur_label.previous_label:
                    break
                cur_label = cur_label.previous_label
                if isinstance(connection, Connection):
                    last_arrival_stop = connection.arrival_stop
            route.append(last_arrival_stop)
            return route

        event_list_raw_data = [
            (1, 2, 0, 10, "trip_1"),
            (2, 3, 10, 20, "trip_1"),
            (4, 5, 30, 40, "trip_2"),

        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(2, 4, {"d_walk": 10})
        walk_network.add_edge(3, 4, {"d_walk": 10})
        walk_speed = 1
        target_stop = 5
        transfer_margin = 0
        start_time = 0
        end_time = 50

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed, track_vehicle_legs=False,
                                                      track_time=True, track_route=True)
        csa_profile.run()
        for stop, profile in csa_profile.stop_profiles.items():
            for label_bag in profile._label_bags:
                for label in label_bag:
                    print('origin:', stop, 'n_boardings/movement_duration:', label.movement_duration, 'route:', unpack_route_from_labels(label))
        print('optimal labels:')
        for stop, profile in csa_profile.stop_profiles.items():
            for label in profile.get_final_optimal_labels():

                print('origin:', stop, 'n_boardings/movement_duration:', label.movement_duration, 'route:', unpack_route_from_labels(label))
                #if stop == 1:
                    #assert 3 not in unpack_route_from_labels(label)
                # print('origin:', stop, 'n_boardings:', label.n_boardings, 'route:', unpack_route_from_labels(label))

    def test_journeys_using_movement_duration_last_stop_walk(self):
        def unpack_route_from_labels(cur_label):
            route = []
            last_arrival_stop = None
            print(cur_label)
            while True:
                print(cur_label.previous_label)
                connection = cur_label.connection
                if isinstance(connection, Connection):
                    route.append(connection.departure_stop)

                if not cur_label.previous_label:
                    break
                cur_label = cur_label.previous_label
                if isinstance(connection, Connection):
                    last_arrival_stop = connection.arrival_stop
            route.append(last_arrival_stop)
            return route

        event_list_raw_data = [
            (1, 2, 0, 10, "trip_1"),
            (2, 3, 10, 20, "trip_2"),
            (4, 5, 30, 40, "trip_3"),

        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(2, 4, {"d_walk": 10})
        walk_network.add_edge(3, 4, {"d_walk": 10})
        walk_network.add_edge(5, 6, {"d_walk": 10})
        walk_speed = 1
        target_stop = 5
        transfer_margin = 0
        start_time = 0
        end_time = 50

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed, track_vehicle_legs=False,
                                                      track_time=True, track_route=True)
        csa_profile.run()
        for stop, profile in csa_profile.stop_profiles.items():
            for label_bag in profile._label_bags:
                for label in label_bag:
                    print('origin:', stop,
                          'n_boardings/movement_duration:', label.movement_duration,
                          'route:', unpack_route_from_labels(label))
        print('optimal labels:')
        for stop, profile in csa_profile.stop_profiles.items():
            for label in profile.get_final_optimal_labels():

                print('origin:', stop,
                      'n_boardings/movement_duration:', label.movement_duration,
                      'route:', unpack_route_from_labels(label))
                #if stop == 1:
                    #assert 3 not in unpack_route_from_labels(label)
                # print('origin:', stop, 'n_boardings:', label.n_boardings, 'route:', unpack_route_from_labels(label))
