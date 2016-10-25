from unittest import TestCase

from gtfspy.routing.label import Label, LabelWithNumberVehicles, compute_pareto_front, merge_pareto_frontiers


class TestLabel(TestCase):

    def test_dominates(self):
        label1 = Label(departure_time=0, arrival_time_target=20)
        label2 = Label(departure_time=1, arrival_time_target=10)
        label3 = Label(departure_time=1, arrival_time_target=10)
        label4 = Label(departure_time=1, arrival_time_target=11)
        label5 = Label(departure_time=0, arrival_time_target=10)
        self.assertTrue(label2.dominates(label1))
        self.assertTrue(label2.dominates(label3))
        self.assertTrue(label2.dominates(label4))
        self.assertTrue(label2.dominates(label5))

    def test_duration(self):
        label1 = Label(departure_time=0, arrival_time_target=20)
        self.assertEqual(20, label1.duration())


class TestLabelWithTransfers(TestCase):

    def test_dominates_simple(self):
        label1 = LabelWithNumberVehicles(departure_time=0, arrival_time_target=20, n_vehicle_legs=0)
        label2 = LabelWithNumberVehicles(departure_time=1, arrival_time_target=10, n_vehicle_legs=0)
        self.assertTrue(label2.dominates(label1))

    def test_does_not_dominate_same(self):
        label2 = LabelWithNumberVehicles(departure_time=1, arrival_time_target=10, n_vehicle_legs=0)
        label3 = LabelWithNumberVehicles(departure_time=1, arrival_time_target=10, n_vehicle_legs=0)
        self.assertTrue(label2.dominates(label3))

    def test_dominates_later_arrival_time(self):
        label2 = LabelWithNumberVehicles(departure_time=1, arrival_time_target=10, n_vehicle_legs=0)
        label4 = LabelWithNumberVehicles(departure_time=1, arrival_time_target=11, n_vehicle_legs=0)
        self.assertTrue(label2.dominates(label4))

    def test_dominates_earlier_departure_time(self):
        label2 = LabelWithNumberVehicles(departure_time=1, arrival_time_target=10, n_vehicle_legs=0)
        label5 = LabelWithNumberVehicles(departure_time=0, arrival_time_target=10, n_vehicle_legs=0)
        self.assertTrue(label2.dominates(label5))

    def test_dominates_less_transfers(self):
        labela = LabelWithNumberVehicles(departure_time=1, arrival_time_target=10, n_vehicle_legs=1)
        labelb = LabelWithNumberVehicles(departure_time=1, arrival_time_target=10, n_vehicle_legs=0)
        self.assertTrue(labelb.dominates(labela))

    def test_duration(self):
        label1 = Label(departure_time=0, arrival_time_target=20)
        self.assertEqual(20, label1.duration())

class TestParetoFrontier(TestCase):

    def test_compute_pareto_front_all_include(self):
        label_a = LabelWithNumberVehicles(departure_time=1, arrival_time_target=2, n_vehicle_legs=0)
        label_b = LabelWithNumberVehicles(departure_time=2, arrival_time_target=3, n_vehicle_legs=0)
        label_c = LabelWithNumberVehicles(departure_time=3, arrival_time_target=4, n_vehicle_legs=0)
        label_d = LabelWithNumberVehicles(departure_time=4, arrival_time_target=5, n_vehicle_legs=0)
        labels = [label_a, label_b, label_c, label_d]
        self.assertEqual(4, len(compute_pareto_front(labels)))

    def test_one_dominates_all(self):
        label_a = LabelWithNumberVehicles(departure_time=1, arrival_time_target=12, n_vehicle_legs=0)
        label_b = LabelWithNumberVehicles(departure_time=2, arrival_time_target=13, n_vehicle_legs=0)
        label_c = LabelWithNumberVehicles(departure_time=3, arrival_time_target=14, n_vehicle_legs=0)
        label_d = LabelWithNumberVehicles(departure_time=4, arrival_time_target=5, n_vehicle_legs=0)
        labels = [label_a, label_b, label_c, label_d]
        pareto_front = compute_pareto_front(labels)
        self.assertEqual(1, len(pareto_front))
        self.assertEqual(label_d, pareto_front[0])

    def test_empty(self):
        labels = []
        self.assertEqual(0, len(compute_pareto_front(labels)))

    def test_some_are_optimal_some_are_not(self):
        label_a = LabelWithNumberVehicles(departure_time=1, arrival_time_target=2, n_vehicle_legs=1)  # optimal
        label_b = LabelWithNumberVehicles(departure_time=1, arrival_time_target=5, n_vehicle_legs=0)  # label_d dominates
        label_c = LabelWithNumberVehicles(departure_time=3, arrival_time_target=4, n_vehicle_legs=1)  # optimal
        label_d = LabelWithNumberVehicles(departure_time=4, arrival_time_target=5, n_vehicle_legs=0)  # optimal
        labels = [label_a, label_b, label_c, label_d]

        pareto_front = compute_pareto_front(labels)
        self.assertEqual(3, len(pareto_front))
        self.assertNotIn(label_b, pareto_front)

    def test_merge_pareto_frontiers(self):
        label_a = LabelWithNumberVehicles(departure_time=1, arrival_time_target=2, n_vehicle_legs=1)  # optimal
        label_b = LabelWithNumberVehicles(departure_time=1, arrival_time_target=5, n_vehicle_legs=0)  # label_d dominates
        label_c = LabelWithNumberVehicles(departure_time=3, arrival_time_target=4, n_vehicle_legs=1)  # optimal
        label_d = LabelWithNumberVehicles(departure_time=4, arrival_time_target=5, n_vehicle_legs=0)  # optimal
        front_1 = [label_a, label_b]
        front_2 = [label_c, label_d]

        pareto_front = merge_pareto_frontiers(front_1, front_2)
        self.assertEqual(3, len(pareto_front))
        self.assertNotIn(label_b, pareto_front)

    def test_merge_pareto_frontiers_empty(self):
        pareto_front = merge_pareto_frontiers([], [])
        self.assertEqual(0, len(pareto_front))
