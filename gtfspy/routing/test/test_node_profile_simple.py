from pyximport import install
install()

from unittest import TestCase

from gtfspy.routing.label import LabelTimeSimple, LabelTimeWithBoardingsCount
from gtfspy.routing.node_profile_simple import NodeProfileSimple


class TestNodeProfileSimple(TestCase):

    def test_earliest_arrival_time(self):
        node_profile = NodeProfileSimple()
        self.assertEqual(float("inf"), node_profile.evaluate_earliest_arrival_time_at_target(0, 0))

        node_profile.update_pareto_optimal_tuples(LabelTimeSimple(departure_time=1, arrival_time_target=1))
        self.assertEqual(1, node_profile.evaluate_earliest_arrival_time_at_target(0, 0))

        node_profile.update_pareto_optimal_tuples(LabelTimeSimple(departure_time=3, arrival_time_target=4))
        self.assertEqual(4, node_profile.evaluate_earliest_arrival_time_at_target(2, 0))

    def test_pareto_optimality(self):
        node_profile = NodeProfileSimple()

        pair1 = LabelTimeSimple(departure_time=1, arrival_time_target=2)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair1))

        pair2 = LabelTimeSimple(departure_time=2, arrival_time_target=3)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair2))

        self.assertEqual(2, len(node_profile._labels))

        pair3 = LabelTimeSimple(departure_time=1, arrival_time_target=1)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair3))
        self.assertEqual(2, len(node_profile._labels), msg=str(node_profile.get_final_optimal_labels()))

        pair4 = LabelTimeSimple(departure_time=1, arrival_time_target=2)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pair4))

    def test_pareto_optimality2(self):
        node_profile = NodeProfileSimple()
        pt2 = LabelTimeSimple(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))
        pt1 = LabelTimeSimple(departure_time=5, arrival_time_target=35)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pt1))
        self.assertEqual(len(node_profile.get_final_optimal_labels()), 1)

    def test_identity_profile(self):
        identity_profile = NodeProfileSimple(0)
        self.assertFalse(identity_profile.update_pareto_optimal_tuples(LabelTimeSimple(10, 10)))
        self.assertEqual(10, identity_profile.evaluate_earliest_arrival_time_at_target(10, 0))

    def test_walk_duration(self):
        node_profile = NodeProfileSimple(walk_to_target_duration=27)
        self.assertEqual(27, node_profile.get_walk_to_target_duration())
        pt1 = LabelTimeSimple(departure_time=5, arrival_time_target=35)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pt1))
        pt2 = LabelTimeSimple(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))

    def test_pareto_optimality_with_transfers(self):
        node_profile = NodeProfileSimple(label_class=LabelTimeWithBoardingsCount)
        pt3 = LabelTimeWithBoardingsCount(departure_time=5, arrival_time_target=35, n_boardings=0, first_leg_is_walk=True)
        pt2 = LabelTimeWithBoardingsCount(departure_time=5, arrival_time_target=35, n_boardings=1, first_leg_is_walk=True)
        pt1 = LabelTimeWithBoardingsCount(departure_time=5, arrival_time_target=35, n_boardings=2, first_leg_is_walk=True)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt1))
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt3))
        self.assertEqual(1, len(node_profile.get_final_optimal_labels()))

