from unittest import TestCase

import networkx

from gtfspy.routing.models import Connection
from gtfspy.routing.label import min_arrival_time_target, LabelWithVehicleCount, Label
from gtfspy.routing.multi_objective_pseudo_connection_scan_profiler import MultiObjectivePseudoCSAProfiler


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
        self.stop_one = 1
        self.target_stop = 4
        self.transfer_margin = 0
        self.start_time = 0
        self.end_time = 50

    def test_basics(self):
        csa_profile = MultiObjectivePseudoCSAProfiler(self.transit_connections, self.target_stop,
                                                      self.start_time, self.end_time, self.transfer_margin,
                                                      self.walk_network, self.walk_speed)
        csa_profile.run()

        stop_3_labels = csa_profile.stop_profiles[3].get_pareto_optimal_labels()
        self.assertEqual(len(stop_3_labels), 2)
        self.assertIn(LabelWithVehicleCount(32, 35, n_vehicle_legs=1), stop_3_labels)

        stop_2_labels = csa_profile.stop_profiles[2].get_pareto_optimal_labels()
        self.assertIn(LabelWithVehicleCount(40, 50, n_vehicle_legs=1), stop_2_labels)
        self.assertIn(LabelWithVehicleCount(25, 35, n_vehicle_legs=2), stop_2_labels)

        stop_one_profile = csa_profile.stop_profiles[self.stop_one]
        stop_one_pareto_labels = stop_one_profile.get_pareto_optimal_labels()

        labels = set()
        labels.add(LabelWithVehicleCount(departure_time=10, arrival_time_target=35, n_vehicle_legs=3))
        labels.add(LabelWithVehicleCount(departure_time=20, arrival_time_target=50, n_vehicle_legs=1))
        labels.add(LabelWithVehicleCount(departure_time=32, arrival_time_target=55, n_vehicle_legs=1))

        for label in labels:
            self.assertIn(label, stop_one_pareto_labels)

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

        labels = set()
        labels.add(LabelWithVehicleCount(departure_time=20, arrival_time_target=50, n_vehicle_legs=1))

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        source_stop_profile = csa_profile.stop_profiles[source_stop]
        source_stop_labels = source_stop_profile.get_pareto_optimal_labels()

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
        labels = set()
        labels.add(LabelWithVehicleCount(departure_time=0, arrival_time_target=30, n_vehicle_legs=1))

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed)
        csa_profile.run()
        found_tuples = csa_profile.stop_profiles[source_stop].get_pareto_optimal_labels()
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
        self.assertEqual(min_arrival_time_target(source_profile.evaluate(0, 0)), 0.5)
        found_tuples = source_profile.get_pareto_optimal_labels()
        print(found_tuples[0])
        self.assertEqual(len(found_tuples), 1)

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
        found_tuples = source_profile.get_pareto_optimal_labels()
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
        found_labels = source_profile.get_pareto_optimal_labels()
        labels_should_be = set()
        labels_should_be.add(LabelWithVehicleCount(0, 10, n_vehicle_legs=1))
        labels_should_be.add(LabelWithVehicleCount(2, 8, n_vehicle_legs=2))
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
        self.assertEqual(1, len(stop_profile_1.get_pareto_optimal_labels()))
        self.assertEqual(1, len(stop_profile_3.get_pareto_optimal_labels()))

        # case with transfer margin
        transfer_margin = 1
        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      networkx.Graph(), walk_speed)
        csa_profile.run()
        stop_profile_3 = csa_profile.stop_profiles[3]
        stop_profile_1 = csa_profile.stop_profiles[1]
        self.assertEqual(0, len(stop_profile_3.get_pareto_optimal_labels()))
        self.assertEqual(1, len(stop_profile_1.get_pareto_optimal_labels()))

    def test_basics_no_transfer_tracking(self):
        csa_profile = MultiObjectivePseudoCSAProfiler(
            self.transit_connections, self.target_stop,
            self.start_time, self.end_time, self.transfer_margin,
            self.walk_network, self.walk_speed, count_transfers=False
        )
        csa_profile.run()

        stop_3_pareto_tuples = csa_profile.stop_profiles[3].get_pareto_optimal_labels()
        self.assertEqual(len(stop_3_pareto_tuples), 1)
        self.assertIn(Label(32, 35), stop_3_pareto_tuples)

        stop_2_pareto_tuples = csa_profile.stop_profiles[2].get_pareto_optimal_labels()
        self.assertEqual(len(stop_2_pareto_tuples), 2)
        self.assertIn(Label(40, 50), stop_2_pareto_tuples)
        self.assertIn(Label(25, 35), stop_2_pareto_tuples)

        source_stop_profile = csa_profile.stop_profiles[self.stop_one]
        source_stop_pareto_optimal_tuples = source_stop_profile.get_pareto_optimal_labels()

        pareto_tuples = set()
        pareto_tuples.add(Label(departure_time=10, arrival_time_target=35))
        pareto_tuples.add(Label(departure_time=20, arrival_time_target=50))
        pareto_tuples.add(Label(departure_time=32, arrival_time_target=55))

        self._assert_label_sets_equal(
            pareto_tuples,
            source_stop_pareto_optimal_tuples
        )

    def _assert_label_sets_equal(self, found_tuples, should_be_tuples):
        self.assertEqual(len(found_tuples), len(should_be_tuples))
        for found_tuple in found_tuples:
            self.assertIn(found_tuple, should_be_tuples)
        for should_be_tuple in should_be_tuples:
            self.assertIn(should_be_tuple, found_tuples)
