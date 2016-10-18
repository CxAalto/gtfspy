from unittest import TestCase

from gtfspy.routing.pareto_tuple import ParetoTuple
from gtfspy.routing.node_profile import NodeProfile


class TestNodeProfile(TestCase):

    def test_earliest_arrival_time(self):
        node_profile = NodeProfile()
        self.assertEquals(float("inf"), node_profile.get_earliest_arrival_time_at_target(0))

        node_profile.update_pareto_optimal_tuples(ParetoTuple(departure_time=1, arrival_time_target=1))
        self.assertEquals(1, node_profile.get_earliest_arrival_time_at_target(0))

    def test_pareto_optimality(self):
        node_profile = NodeProfile()

        pair1 = ParetoTuple(departure_time=1, arrival_time_target=2)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair1))

        pair2 = ParetoTuple(departure_time=2, arrival_time_target=3)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair2))

        self.assertEquals(2, len(node_profile._pareto_tuples))

        pair3 = ParetoTuple(departure_time=1, arrival_time_target=1)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pair3))
        self.assertEquals(2, len(node_profile._pareto_tuples), msg=str(node_profile.get_pareto_tuples()))

        pair4 = ParetoTuple(departure_time=1, arrival_time_target=2)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pair4))

    def test_pareto_optimality2(self):
        node_profile = NodeProfile()
        pt2 = ParetoTuple(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))
        pt1 = ParetoTuple(departure_time=5, arrival_time_target=35)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pt1))
        self.assertEquals(len(node_profile.get_pareto_tuples()), 1)

    def test_identity_profile(self):
        identity_profile = NodeProfile(0)
        self.assertFalse(identity_profile.update_pareto_optimal_tuples(ParetoTuple(10, 10)))
        self.assertEqual(10, identity_profile.get_earliest_arrival_time_at_target(10))

    def test_walk_duration(self):
        node_profile = NodeProfile(walk_to_target_duration=27)
        self.assertEqual(27, node_profile.get_walk_to_target_duration())
        pt1 = ParetoTuple(departure_time=5, arrival_time_target=35)
        self.assertFalse(node_profile.update_pareto_optimal_tuples(pt1))
        pt2 = ParetoTuple(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update_pareto_optimal_tuples(pt2))
