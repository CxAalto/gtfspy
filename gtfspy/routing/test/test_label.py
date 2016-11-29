import copy
import pyximport

pyximport.install()

from unittest import TestCase

from gtfspy.routing.label import LabelTime, LabelTimeAndVehLegCount, merge_pareto_frontiers, \
    LabelVehLegCount, compute_pareto_front, compute_pareto_front_naive


class TestLabelTime(TestCase):

    def test_dominates(self):
        label1 = LabelTime(departure_time=0, arrival_time_target=20)
        label2 = LabelTime(departure_time=1, arrival_time_target=10)
        label3 = LabelTime(departure_time=1, arrival_time_target=10)
        label4 = LabelTime(departure_time=1, arrival_time_target=11)
        label5 = LabelTime(departure_time=0, arrival_time_target=10)
        self.assertTrue(label2.dominates(label1))
        self.assertTrue(label2.dominates(label3))
        self.assertTrue(label2.dominates(label4))
        self.assertTrue(label2.dominates(label5))

    def test_duration(self):
        label1 = LabelTime(departure_time=0, arrival_time_target=20)
        self.assertEqual(20, label1.duration())

    def test_equal(self):
        label1 = LabelTime(departure_time=0, arrival_time_target=20)
        label2 = LabelTime(departure_time=0, arrival_time_target=20)
        label3 = LabelTime(departure_time=10, arrival_time_target=20)
        self.assertEqual(label1, label2)
        self.assertNotEqual(label1, label3)

    def test_sort(self):
        l1 = LabelTime(departure_time=1, arrival_time_target=1)
        l2 = LabelTime(0, 0)
        self.assertTrue(l1 > l2)
        self.assertTrue(l1 >= l2)
        self.assertFalse(l1 < l2)
        self.assertFalse(l1 <= l2)

        l1 = LabelTime(0, 0)
        l2 = LabelTime(0, 0)
        self.assertTrue(l1 == l2)
        self.assertTrue(l1 >= l2)
        self.assertTrue(l1 <= l2)
        self.assertFalse(l1 != l2)

        l1 = LabelTime(1, 0)
        l2 = LabelTime(1, 1)
        self.assertTrue(l1 > l2)
        self.assertFalse(l1 < l2)

        self.assertTrue(sorted([l1, l2])[0] == l2)


class TestLabelTimeAndVehLegCount(TestCase):

    def test_dominates_simple(self):
        label1 = LabelTimeAndVehLegCount(departure_time=0, arrival_time_target=20, n_vehicle_legs=0, first_leg_is_walk=False)
        label2 = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=10, n_vehicle_legs=0, first_leg_is_walk=False)
        self.assertTrue(label2.dominates(label1))

    def test_does_not_dominate_same(self):
        label2 = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=10, n_vehicle_legs=0, first_leg_is_walk=False)
        label3 = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=10, n_vehicle_legs=0, first_leg_is_walk=False)
        self.assertTrue(label2.dominates(label3))

    def test_dominates_later_arrival_time(self):
        label2 = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=10, n_vehicle_legs=0, first_leg_is_walk=False)
        label4 = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=11, n_vehicle_legs=0, first_leg_is_walk=False)
        self.assertTrue(label2.dominates(label4))

    def test_dominates_earlier_departure_time(self):
        label2 = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=10, n_vehicle_legs=0, first_leg_is_walk=False)
        label5 = LabelTimeAndVehLegCount(departure_time=0, arrival_time_target=10, n_vehicle_legs=0, first_leg_is_walk=False)
        self.assertTrue(label2.dominates(label5))

    def test_dominates_less_transfers(self):
        labela = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=10, n_vehicle_legs=1, first_leg_is_walk=False)
        labelb = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=10, n_vehicle_legs=0, first_leg_is_walk=False)
        self.assertTrue(labelb.dominates(labela))

    def test_duration(self):
        label1 = LabelTime(departure_time=0, arrival_time_target=20, last_leg_is_walk=False)
        self.assertEqual(20, label1.duration())

    def test_sort(self):
        l1 = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=1, n_vehicle_legs=3, first_leg_is_walk=False)
        l2 = LabelTimeAndVehLegCount(0, 0, 0, False)
        self.assertTrue(l1 > l2)
        self.assertTrue(l1 >= l2)
        self.assertFalse(l1 < l2)
        self.assertFalse(l1 <= l2)

        l1 = LabelTimeAndVehLegCount(0, 0, 0, False)
        l2 = LabelTimeAndVehLegCount(0, 0, 0, False)
        self.assertTrue(l1 == l2)
        self.assertTrue(l1 >= l2)
        self.assertTrue(l1 <= l2)
        self.assertFalse(l1 != l2)

        l1 = LabelTimeAndVehLegCount(1, 0, 10, False)
        l2 = LabelTimeAndVehLegCount(1, 1, 10, False)
        self.assertTrue(l1 > l2)
        self.assertFalse(l1 < l2)

        l1 = LabelTimeAndVehLegCount(1, 1, 0, False)
        l2 = LabelTimeAndVehLegCount(1, 1, 10, False)
        self.assertTrue(l1 > l2)
        self.assertFalse(l1 < l2)

        self.assertTrue(sorted([l1, l2])[0] == l2)


        l1 = LabelTimeAndVehLegCount(1, 1, 10, True)
        l2 = LabelTimeAndVehLegCount(1, 1, 10, False)
        self.assertTrue(l1 < l2)
        self.assertFalse(l1 > l2)


class TestLabelVehLegCount(TestCase):

    def test_dominates_simple(self):
        label1 = LabelVehLegCount(n_vehicle_legs=1)
        label2 = LabelVehLegCount(n_vehicle_legs=0)
        self.assertTrue(label2.dominates(label1))

    def test_sort(self):
        l1 = LabelVehLegCount(departure_time=1, n_vehicle_legs=3)
        l2 = LabelVehLegCount(departure_time=0, n_vehicle_legs=0)
        self.assertTrue(l2 > l1)
        self.assertTrue(l2 >= l1)
        self.assertFalse(l2 < l1)
        self.assertFalse(l2 <= l1)

        l1 = LabelVehLegCount(departure_time=0, n_vehicle_legs=0)
        l2 = LabelVehLegCount(departure_time=0, n_vehicle_legs=0)
        self.assertTrue(l1 == l2)
        self.assertTrue(l1 >= l2)
        self.assertTrue(l1 <= l2)
        self.assertFalse(l1 != l2)

        l1 = LabelVehLegCount(departure_time=0, n_vehicle_legs=1)
        l2 = LabelVehLegCount(departure_time=0, n_vehicle_legs=0)
        self.assertFalse(l1 > l2)
        self.assertTrue(l1 < l2)
        self.assertTrue(l1 <= l2)
        self.assertTrue(l1 != l2)

        self.assertTrue(sorted([l1, l2])[0] == l1)

    def test_pareto_frontier(self):
        pt3 = LabelVehLegCount(departure_time=5, n_vehicle_legs=0)
        pt2 = LabelVehLegCount(departure_time=6, n_vehicle_legs=1)
        pt1 = LabelVehLegCount(departure_time=7, n_vehicle_legs=2)
        labels = [pt1, pt2, pt3]
        self.assertEqual(1, len(compute_pareto_front(labels)))

class TestParetoFrontier(TestCase):

    def test_compute_pareto_front_all_include(self):
        label_a = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=2, n_vehicle_legs=0, first_leg_is_walk=False)
        label_b = LabelTimeAndVehLegCount(departure_time=2, arrival_time_target=3, n_vehicle_legs=0, first_leg_is_walk=False)
        label_c = LabelTimeAndVehLegCount(departure_time=3, arrival_time_target=4, n_vehicle_legs=0, first_leg_is_walk=False)
        label_d = LabelTimeAndVehLegCount(departure_time=4, arrival_time_target=5, n_vehicle_legs=0, first_leg_is_walk=False)
        labels = [label_a, label_b, label_c, label_d]
        self.assertEqual(4, len(compute_pareto_front(labels)))

    def test_one_dominates_all(self):
        label_a = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=12, n_vehicle_legs=0, first_leg_is_walk=False)
        label_b = LabelTimeAndVehLegCount(departure_time=2, arrival_time_target=13, n_vehicle_legs=0, first_leg_is_walk=False)
        label_c = LabelTimeAndVehLegCount(departure_time=3, arrival_time_target=14, n_vehicle_legs=0, first_leg_is_walk=False)
        label_d = LabelTimeAndVehLegCount(departure_time=4, arrival_time_target=5, n_vehicle_legs=0, first_leg_is_walk=False)
        labels = [label_a, label_b, label_c, label_d]
        pareto_front = compute_pareto_front(labels)
        self.assertEqual(1, len(pareto_front))
        self.assertEqual(label_d, pareto_front[0])

    def test_empty(self):
        labels = []
        self.assertEqual(0, len(compute_pareto_front(labels)))

    def test_some_are_optimal_some_are_not(self):
        label_a = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=2, n_vehicle_legs=1, first_leg_is_walk=False)  # optimal
        label_b = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=5, n_vehicle_legs=0, first_leg_is_walk=False)  # label_d dominates
        label_c = LabelTimeAndVehLegCount(departure_time=3, arrival_time_target=4, n_vehicle_legs=1, first_leg_is_walk=False)  # optimal
        label_d = LabelTimeAndVehLegCount(departure_time=4, arrival_time_target=5, n_vehicle_legs=0, first_leg_is_walk=False)  # optimal
        labels = [label_a, label_b, label_c, label_d]

        pareto_front = compute_pareto_front(labels)
        self.assertEqual(3, len(pareto_front))
        self.assertNotIn(label_b, pareto_front)

    def test_merge_pareto_frontiers(self):
        label_a = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=2, n_vehicle_legs=1, first_leg_is_walk=False)  # optimal
        label_b = LabelTimeAndVehLegCount(departure_time=1, arrival_time_target=5, n_vehicle_legs=0, first_leg_is_walk=False)  # d dominates
        label_c = LabelTimeAndVehLegCount(departure_time=3, arrival_time_target=4, n_vehicle_legs=1, first_leg_is_walk=False)  # optimal
        label_d = LabelTimeAndVehLegCount(departure_time=4, arrival_time_target=5, n_vehicle_legs=0, first_leg_is_walk=False)  # optimal
        front_1 = [label_a, label_b]
        front_2 = [label_c, label_d]

        pareto_front = merge_pareto_frontiers(front_1, front_2)
        self.assertEqual(3, len(pareto_front))
        self.assertNotIn(label_b, pareto_front)

    def test_merge_pareto_frontiers_empty(self):
        pareto_front = merge_pareto_frontiers([], [])
        self.assertEqual(0, len(pareto_front))

    def test_compute_pareto_front_smart(self):
        labels = []
        for n in [1, 2, 10]: #, 500]:
            for dep_time in range(0, n):
                for n_veh_legs in range(2):
                    for arr_time in range(dep_time, dep_time  + 10):
                        label = LabelTimeAndVehLegCount(dep_time, arr_time-n_veh_legs, n_veh_legs, False)
                        labels.append(label)
            import random
            random.shuffle(labels)
            labels_copy = copy.deepcopy(labels)
            pareto_optimal_labels = compute_pareto_front_naive(labels)
            self.assertEqual(len(pareto_optimal_labels), n * 2)
            pareto_optimal_labels = compute_pareto_front(labels_copy)
            self.assertEqual(len(pareto_optimal_labels), n * 2)

    def test_compute_pareto_front_smart_randomized(self):
        import random
        for i in range(10):
            labels = [LabelTimeAndVehLegCount(random.randint(0, 1000), random.randint(0, 1000), random.randint(0, 10), 0)
                      for _ in range(1000)]
            pareto_optimal_labels_old = compute_pareto_front_naive(labels)
            pareto_optimal_labels_smart = compute_pareto_front(labels)
            self.assertEqual(len(pareto_optimal_labels_old), len(pareto_optimal_labels_smart))

