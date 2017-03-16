import pyximport
pyximport.install()

from unittest import TestCase

from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTime, min_arrival_time_target, LabelTimeWithBoardingsCount, LabelVehLegCount


class TestNodeProfileMultiObjective(TestCase):

    def test_evaluate(self):
        node_profile = NodeProfileMultiObjective(dep_times=[3, 1], label_class=LabelTime)

        node_profile.update([LabelTime(departure_time=3, arrival_time_target=4)])
        self.assertEquals(4, min_arrival_time_target(node_profile.evaluate(3)))

        node_profile.update([LabelTime(departure_time=1, arrival_time_target=1)])
        self.assertEquals(1, min_arrival_time_target(node_profile.evaluate(1)))

    def test_pareto_optimality2(self):
        node_profile = NodeProfileMultiObjective(dep_times=[5, 10], label_class=LabelTime)
        pt2 = LabelTime(departure_time=10, arrival_time_target=35, last_leg_is_walk=False)
        node_profile.update([pt2])
        pt1 = LabelTime(departure_time=5, arrival_time_target=35, last_leg_is_walk=False)
        node_profile.update([pt1])
        self.assertEquals(len(node_profile.get_labels_for_real_connections()), 1)

    def test_identity_profile(self):
        identity_profile = NodeProfileMultiObjective(dep_times=[10])
        identity_profile.update([LabelTimeWithBoardingsCount(10, 10, 0, True)])
        self.assertEqual(10, min_arrival_time_target(identity_profile.evaluate(10, first_leg_can_be_walk=True)))

    def test_walk_duration(self):
        node_profile = NodeProfileMultiObjective(dep_times=[10, 5], walk_to_target_duration=27, label_class=LabelTime)
        self.assertEqual(27, node_profile.get_walk_to_target_duration())
        pt2 = LabelTime(departure_time=10, arrival_time_target=35)
        pt1 = LabelTime(departure_time=5, arrival_time_target=35)
        node_profile.update([pt2])
        node_profile.update([pt1])

    def test_pareto_optimality_with_transfers_and_time(self):
        node_profile = NodeProfileMultiObjective(dep_times=[5, 6, 7])
        pt3 = LabelTimeWithBoardingsCount(departure_time=5, arrival_time_target=45, n_boardings=0, first_leg_is_walk=False)
        pt2 = LabelTimeWithBoardingsCount(departure_time=6, arrival_time_target=40, n_boardings=1, first_leg_is_walk=False)
        pt1 = LabelTimeWithBoardingsCount(departure_time=7, arrival_time_target=35, n_boardings=2, first_leg_is_walk=False)
        self.assertTrue(node_profile.update([pt1]))
        self.assertTrue(node_profile.update([pt2]))
        self.assertTrue(node_profile.update([pt3]))
        self.assertEqual(3, len(node_profile.get_labels_for_real_connections()))

    def test_pareto_optimality_with_transfers_only(self):
        LabelClass = LabelVehLegCount
        node_profile = NodeProfileMultiObjective(dep_times=[5, 6, 7], label_class=LabelClass)
        pt3 = LabelClass(departure_time=5, n_vehicle_legs=0, last_leg_is_walk=False)
        pt2 = LabelClass(departure_time=6, n_vehicle_legs=1, last_leg_is_walk=False)
        pt1 = LabelClass(departure_time=7, n_vehicle_legs=2, last_leg_is_walk=False)
        self.assertTrue(node_profile.update([pt1]))
        self.assertTrue(node_profile.update([pt2]))
        self.assertTrue(node_profile.update([pt3]))
        node_profile.finalize()
        self.assertEqual(1, len(node_profile.get_final_optimal_labels()))

    def test_finalize(self):
        node_profile = NodeProfileMultiObjective(label_class=LabelTimeWithBoardingsCount, dep_times=[10])
        own_label = LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=20, n_boardings=0, first_leg_is_walk=False)
        self.assertTrue(node_profile.update([own_label]))
        neighbor_label = LabelTimeWithBoardingsCount(departure_time=15, arrival_time_target=18, n_boardings=2, first_leg_is_walk=False)
        assert(len(node_profile.get_labels_for_real_connections()) == 1)
        node_profile.finalize([[neighbor_label]], [3])
        assert (len(node_profile.get_final_optimal_labels()) == 2)
        self.assertTrue(any(map(lambda el: el.departure_time == 12, node_profile.get_final_optimal_labels())))

    def test_same_dep_times_fail_in_init(self):
        with self.assertRaises(AssertionError):
            node_profile = NodeProfileMultiObjective(label_class=LabelTimeWithBoardingsCount, dep_times=[10, 10, 20, 20])


    def test_dep_time_skipped_in_update(self):
        label3 = LabelTimeWithBoardingsCount(departure_time=30, arrival_time_target=20, n_boardings=0,
                                             first_leg_is_walk=False)
        label2 = LabelTimeWithBoardingsCount(departure_time=20, arrival_time_target=20, n_boardings=0,
                                             first_leg_is_walk=False)
        label1 = LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=20, n_boardings=0,
                                             first_leg_is_walk=False)

        # This should work ok
        node_profile = NodeProfileMultiObjective(label_class=LabelTimeWithBoardingsCount, dep_times=[10, 20, 30])
        node_profile.update([label3])
        node_profile.update([label2])
        node_profile.update([label2])
        node_profile.update([label1])

        # This should fail due to dep time 20 missing in between
        with self.assertRaises(AssertionError):
            node_profile = NodeProfileMultiObjective(label_class=LabelTimeWithBoardingsCount, dep_times=[10, 20, 30])
            node_profile.update([label3])
            node_profile.update([label1])

        # This should fail due to dep time 30 not being the first to deal with
        with self.assertRaises(AssertionError):
            node_profile = NodeProfileMultiObjective(label_class=LabelTimeWithBoardingsCount, dep_times=[10, 20, 30])
            node_profile.update([label2])

