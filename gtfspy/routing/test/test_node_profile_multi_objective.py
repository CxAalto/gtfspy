from unittest import TestCase

from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTime, min_arrival_time_target, LabelTimeAndVehLegCount, LabelVehLegCount


class TestNodeProfileMultiObjective(TestCase):

    def test_evaluate(self):
        node_profile = NodeProfileMultiObjective(dep_times=[3, 1])

        node_profile.update([LabelTime(departure_time=3, arrival_time_target=4)])
        self.assertEquals(4, min_arrival_time_target(node_profile.evaluate(3)))

        node_profile.update([LabelTime(departure_time=1, arrival_time_target=1)])
        self.assertEquals(1, min_arrival_time_target(node_profile.evaluate(1)))

    def test_pareto_optimality2(self):
        node_profile = NodeProfileMultiObjective(dep_times=[5, 10])
        pt2 = LabelTime(departure_time=10, arrival_time_target=35)
        node_profile.update([pt2])
        pt1 = LabelTime(departure_time=5, arrival_time_target=35)
        node_profile.update([pt1])
        self.assertEquals(len(node_profile.get_labels_for_real_connections()), 1)

    def test_identity_profile(self):
        identity_profile = NodeProfileMultiObjective(dep_times=[10])
        identity_profile.update([LabelTimeAndVehLegCount(10, 10)])
        self.assertEqual(10, min_arrival_time_target(identity_profile.evaluate(10, 0)))

    def test_walk_duration(self):
        node_profile = NodeProfileMultiObjective(dep_times=[10,5], walk_to_target_duration=27, label_class=LabelTime)
        self.assertEqual(27, node_profile.get_walk_to_target_duration())
        pt2 = LabelTime(departure_time=10, arrival_time_target=35)
        pt1 = LabelTime(departure_time=5, arrival_time_target=35)
        node_profile.update([pt2])
        node_profile.update([pt1])

    def test_pareto_optimality_with_transfers_and_time(self):
        node_profile = NodeProfileMultiObjective(dep_times=[5,6,7])
        pt3 = LabelTimeAndVehLegCount(departure_time=5, arrival_time_target=45, n_vehicle_legs=0)
        pt2 = LabelTimeAndVehLegCount(departure_time=6, arrival_time_target=40, n_vehicle_legs=1)
        pt1 = LabelTimeAndVehLegCount(departure_time=7, arrival_time_target=35, n_vehicle_legs=2)
        self.assertTrue(node_profile.update([pt1]))
        self.assertTrue(node_profile.update([pt2]))
        self.assertTrue(node_profile.update([pt3]))
        self.assertEqual(3, len(node_profile.get_labels_for_real_connections()))

    def test_pareto_optimality_with_transfers_only(self):
        LabelClass = LabelVehLegCount
        node_profile = NodeProfileMultiObjective(dep_times=[5, 6, 7], label_class=LabelClass)
        pt3 = LabelClass(departure_time=5, arrival_time_target=35, n_vehicle_legs=0)
        pt2 = LabelClass(departure_time=6, arrival_time_target=35, n_vehicle_legs=1)
        pt1 = LabelClass(departure_time=7, arrival_time_target=35, n_vehicle_legs=2)
        self.assertTrue(node_profile.update([pt1]))
        self.assertTrue(node_profile.update([pt2]))
        self.assertTrue(node_profile.update([pt3]))
        node_profile.finalize()
        self.assertEqual(1, len(node_profile.get_final_optimal_labels()))

    def test_finalize(self):
        NodeProfileMultiObjective()
        node_profile = NodeProfileMultiObjective(label_class=LabelTimeAndVehLegCount, dep_times=[10])
        own_label = LabelTimeAndVehLegCount(departure_time=10, arrival_time_target=20, n_vehicle_legs=0)
        self.assertTrue(node_profile.update([own_label]))
        neighbor_label = LabelTimeAndVehLegCount(departure_time=15, arrival_time_target=18, n_vehicle_legs=2)
        assert(len(node_profile.get_labels_for_real_connections()) == 1)
        node_profile.finalize([[neighbor_label]], [3])
        assert (len(node_profile.get_final_optimal_labels()) == 2)
        self.assertTrue(any(map(lambda el: el.departure_time == 12, node_profile.get_final_optimal_labels())))




