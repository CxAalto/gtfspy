import unittest

from gtfspy.routing.journey import Journey
from gtfspy.routing.models import Connection

class JourneyTest(unittest.TestCase):

    def setUp(self):
        event_list_raw_data = [
            (2, 4, 40, 50, "trip_6"),
            (1, 3, 32, 40, "trip_5"),
            (3, 4, 32, 35, "trip_4"),
            (2, 3, 25, 30, "trip_3"),
            (1, 2, 10, 20, "trip_2"),
            (1, 2, 10, 20, None),
            (0, 1, 0, 10, "trip_1")
        ]
        self.legs = map(lambda el: Connection(*el), event_list_raw_data)
        self.test_journey = Journey()
        for leg in self.legs:
            self.test_journey.add_leg(leg)

    def test_basics(self):
        self.assertIsInstance(self.test_journey, Journey)
        self.assertIsInstance(self.test_journey.get_journey(), list)

        self.assertEqual(self.test_journey.get_boardings(), 6)
        self.assertEqual(self.test_journey.get_transfers(), 5)
        self.assertEqual(self.test_journey.get_travel_time(), -30)
        self.assertIsInstance(self.test_journey.get_waiting_times(), list)
        self.assertEqual(self.test_journey.get_total_waiting_time(), -86)
        self.assertEqual(len(self.test_journey.get_all_stops()), 8)
        self.assertEqual(len(self.test_journey.get_transfer_stop_pairs()), 5)

