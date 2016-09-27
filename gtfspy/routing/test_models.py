from unittest import TestCase

from gtfspy.routing.models import ParetoTuple


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

