from unittest import TestCase

from gtfspy.routing.label import LabelTime, LabelTimeAndVehLegCount
from gtfspy.routing.node_profile_naive import NodeProfileNaive


class TestNodeProfile(TestCase):

    def test_earliest_arrival_time(self):
        node_profile = NodeProfileNaive()
        self.assertEquals(float("inf"), node_profile.evaluate_earliest_arrival_time_at_target(0, 0))

        node_profile.update_pareto_optimal_tuples(LabelTime(departure_time=1, arrival_time_target=1))
        self.assertEquals(1, node_profile.evaluate_earliest_arrival_time_at_target(0, 0))

        node_profile.update_pareto_optimal_tuples(LabelTime(departure_time=3, arrival_time_target=4))
        self.assertEquals(4, node_profile.evaluate_earliest_arrival_time_at_target(2, 0))

    def test_pareto_optimality(self):
        node_profile = NodeProfileNaive()

        pair1 = LabelTime(departure_time=1, arrival_time_target=2)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair1))

        pair2 = LabelTime(departure_time=2, arrival_time_target=3)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair2))

        self.assertEquals(2, len(node_profile._labels))

        pair3 = LabelTime(departure_time=1, arrival_time_target=1)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair3))
        self.assertEquals(2, len(node_profile._labels), msg=str(node_profile.get_pareto_optimal_labels()))

        pair4 = LabelTime(departure_time=1, arrival_time_target=2)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pair4))

    def test_pareto_optimality2(self):
        node_profile = NodeProfileNaive()
        pt2 = LabelTime(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))
        pt1 = LabelTime(departure_time=5, arrival_time_target=35)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pt1))
        self.assertEquals(len(node_profile.get_pareto_optimal_labels()), 1)

    def test_identity_profile(self):
        identity_profile = NodeProfileNaive(0)
        self.assertFalse(identity_profile.update_pareto_optimal_tuples(LabelTime(10, 10)))
        self.assertEqual(10, identity_profile.evaluate_earliest_arrival_time_at_target(10, 0))

    def test_walk_duration(self):
        node_profile = NodeProfileNaive(walk_to_target_duration=27)
        self.assertEqual(27, node_profile.get_walk_to_target_duration())
        pt1 = LabelTime(departure_time=5, arrival_time_target=35)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pt1))
        pt2 = LabelTime(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))

    def test_pareto_optimality_with_transfers(self):
        node_profile = NodeProfileNaive()
        pt3 = LabelTimeAndVehLegCount(departure_time=5, arrival_time_target=35, n_vehicle_legs=0)
        pt2 = LabelTimeAndVehLegCount(departure_time=5, arrival_time_target=35, n_vehicle_legs=1)
        pt1 = LabelTimeAndVehLegCount(departure_time=5, arrival_time_target=35, n_vehicle_legs=2)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt1))
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt3))
        self.assertEqual(1, len(node_profile.get_pareto_optimal_labels()))

