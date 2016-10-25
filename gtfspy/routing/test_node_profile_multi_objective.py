from unittest import TestCase

from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from routing.label import Label, min_arrival_time_target, LabelWithTransfers


class TestNodeProfileMultiObjective(TestCase):

    def test_earliest_arrival_time(self):
        node_profile = NodeProfileMultiObjective()
        self.assertEquals(float("inf"), min_arrival_time_target(node_profile.evaluate(0, 0)))

        node_profile.update({Label(departure_time=3, arrival_time_target=4)})
        self.assertEquals(4, min_arrival_time_target(node_profile.evaluate(2, 0)))

        node_profile.update({Label(departure_time=1, arrival_time_target=1)})
        self.assertEquals(1, min_arrival_time_target(node_profile.evaluate(0, 0)))

    def test_pareto_optimality2(self):
        node_profile = NodeProfileMultiObjective()
        pt2 = Label(departure_time=10, arrival_time_target=35)
        node_profile.update(pt2)
        pt1 = Label(departure_time=5, arrival_time_target=35)
        node_profile.update(pt1)
        self.assertEquals(len(node_profile.get_pareto_optimal_tuples()), 1)

    def test_identity_profile(self):
        identity_profile = NodeProfileMultiObjective(0)
        identity_profile.update({Label(10, 10)})
        self.assertEqual(10, min_arrival_time_target(identity_profile.evaluate(10, 0)))

    def test_walk_duration(self):
        node_profile = NodeProfileMultiObjective(walk_to_target_duration=27)
        self.assertEqual(27, node_profile.get_walk_to_target_duration())
        pt1 = Label(departure_time=5, arrival_time_target=35)
        node_profile.update(pt1)
        pt2 = Label(departure_time=10, arrival_time_target=35)
        self.assertTrue(node_profile.update(pt2))

    def test_pareto_optimality_with_transfers(self):
        node_profile = NodeProfileMultiObjective()
        pt3 = LabelWithTransfers(departure_time=5, arrival_time_target=35, n_transfers=0)
        pt2 = LabelWithTransfers(departure_time=5, arrival_time_target=35, n_transfers=1)
        pt1 = LabelWithTransfers(departure_time=5, arrival_time_target=35, n_transfers=2)
        self.assertTrue(node_profile.update(pt1))
        self.assertTrue(node_profile.update(pt2))
        self.assertTrue(node_profile.update(pt3))
        self.assertEqual(1, len(node_profile.get_pareto_optimal_tuples()))



