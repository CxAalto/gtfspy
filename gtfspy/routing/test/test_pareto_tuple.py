from unittest import TestCase

from gtfspy.routing.pareto_tuple import ParetoTuple, ParetoTupleWithTransfers


class TestParetoTuple(TestCase):

    def test_dominates(self):
        pt1 = ParetoTuple(departure_time=0, arrival_time_target=20)
        pt2 = ParetoTuple(departure_time=1, arrival_time_target=10)
        pt3 = ParetoTuple(departure_time=1, arrival_time_target=10)
        pt4 = ParetoTuple(departure_time=1, arrival_time_target=11)
        pt5 = ParetoTuple(departure_time=0, arrival_time_target=10)
        self.assertTrue(pt2.dominates(pt1))
        self.assertFalse(pt2.dominates(pt3))
        self.assertTrue(pt2.dominates(pt4))
        self.assertTrue(pt2.dominates(pt5))

    def test_duration(self):
        pt1 = ParetoTuple(departure_time=0, arrival_time_target=20)
        self.assertEqual(20, pt1.duration())


class TestParetoTupleWithTransfers(TestCase):

    def test_dominates_simple(self):
        pt1 = ParetoTupleWithTransfers(departure_time=0, arrival_time_target=20, n_transfers=0)
        pt2 = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=10, n_transfers=0)
        self.assertTrue(pt2.dominates(pt1))

    def test_does_not_dominate_same(self):
        pt2 = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=10, n_transfers=0)
        pt3 = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=10, n_transfers=0)
        self.assertFalse(pt2.dominates(pt3))

    def test_dominates_later_arrival_time(self):
        pt2 = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=10, n_transfers=0)
        pt4 = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=11, n_transfers=0)
        self.assertTrue(pt2.dominates(pt4))

    def test_dominates_earlier_departure_time(self):
        pt2 = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=10, n_transfers=0)
        pt5 = ParetoTupleWithTransfers(departure_time=0, arrival_time_target=10, n_transfers=0)
        self.assertTrue(pt2.dominates(pt5))

    def test_dominates_less_transfers(self):
        pta = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=10, n_transfers=1)
        ptb = ParetoTupleWithTransfers(departure_time=1, arrival_time_target=10, n_transfers=0)
        self.assertTrue(ptb.dominates(pta))

    def test_duration(self):
        pt1 = ParetoTuple(departure_time=0, arrival_time_target=20)
        self.assertEqual(20, pt1.duration())



