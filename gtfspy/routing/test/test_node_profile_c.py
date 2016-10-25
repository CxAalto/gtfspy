from unittest import TestCase

from gtfspy.routing.label import Label
from gtfspy.routing.node_profile_c import NodeProfileC


class TestNodeProfileC(TestCase):

    def test_earliest_arrival_time(self):
        node_profile = NodeProfileC()
        self.assertEquals(float("inf"), node_profile.evaluate_earliest_arrival_time_at_target(0, 0))

        node_profile.update_pareto_optimal_tuples(Label(departure_time=3, arrival_time_target=4))
        self.assertEquals(4, node_profile.evaluate_earliest_arrival_time_at_target(2, 0))

        node_profile.update_pareto_optimal_tuples(Label(departure_time=1, arrival_time_target=1))
        self.assertEquals(1, node_profile.evaluate_earliest_arrival_time_at_target(0, 0))

    def test_pareto_optimality(self):
        node_profile = NodeProfileC()

        pair3 = Label(departure_time=2, arrival_time_target=3)
        node_profile.update_pareto_optimal_tuples(pair3)
        pair2 = Label(departure_time=1, arrival_time_target=2)
        node_profile.update_pareto_optimal_tuples(pair2)
        pair1 = Label(departure_time=1, arrival_time_target=1)
        node_profile.update_pareto_optimal_tuples(pair1)
        self.assertEqual(2, len(node_profile.get_pareto_optimal_tuples()))

    def test_pareto_optimality2(self):
        node_profile = NodeProfileC()
        pt2 = Label(departure_time=10, arrival_time_target=35)
        node_profile.update_pareto_optimal_tuples(pt2)
        pt1 = Label(departure_time=5, arrival_time_target=35)
        node_profile.update_pareto_optimal_tuples(pt1)
        print(node_profile._labels)
        self.assertEquals(len(node_profile.get_pareto_optimal_tuples()), 1)

    def test_identity_profile(self):
        identity_profile = NodeProfileC(0)
        self.assertFalse(identity_profile.update_pareto_optimal_tuples(Label(10, 10)))
        self.assertEqual(10, identity_profile.evaluate_earliest_arrival_time_at_target(10, 0))

    def test_walk_duration(self):
        node_profile = NodeProfileC(walk_to_target_duration=27)
        self.assertEqual(27, node_profile.get_walk_to_target_duration())
        pt2 = Label(departure_time=10, arrival_time_target=35)
        pt1 = Label(departure_time=5, arrival_time_target=35)
        node_profile.update_pareto_optimal_tuples(pt2)
        node_profile.update_pareto_optimal_tuples(pt1)
        self.assertEqual(1, len(node_profile.get_pareto_optimal_tuples()))

    def test_assert_raises_wrong_order(self):
        node_profile = NodeProfileC()
        pt1 = Label(departure_time=5, arrival_time_target=35)
        pt2 = Label(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt1))
        with self.assertRaises(AssertionError):
            node_profile.update_pareto_optimal_tuples(pt2)
