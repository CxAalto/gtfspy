import unittest

from gtfspy.routing.forwardjourney import ForwardJourney
from gtfspy.routing.connection import Connection


class ForwardJourneyTest(unittest.TestCase):

    def test_add_leg(self):
        journey = ForwardJourney()
        leg1 = Connection(departure_stop=0, arrival_stop=1, departure_time=0, arrival_time=1,
                          trip_id="tripI", seq=1, is_walk=False)
        journey.add_leg(leg1)
        self.assertEqual(len(journey.legs), 1)
        self.assertEqual(journey.departure_time, leg1.departure_time)
        self.assertEqual(journey.arrival_time, leg1.arrival_time)
        self.assertEqual(journey.n_boardings, 1)

        leg2 = Connection(departure_stop=1, arrival_stop=2, departure_time=1, arrival_time=2,
                          trip_id="tripI", seq=1, is_walk=False)
        journey.add_leg(leg2)
        self.assertEqual(len(journey.legs), 2)
        self.assertEqual(journey.departure_time, leg1.departure_time)
        self.assertEqual(journey.arrival_time, leg2.arrival_time)
        self.assertEqual(journey.n_boardings, 1)

    def test_dominates(self):
        leg1 = Connection(departure_stop=0, arrival_stop=1, departure_time=0, arrival_time=1,
                          trip_id="tripI", seq=1, is_walk=False)
        leg2 = Connection(departure_stop=1, arrival_stop=2, departure_time=1, arrival_time=2,
                          trip_id="tripI", seq=1, is_walk=False)
        leg3 = Connection(departure_stop=1, arrival_stop=2, departure_time=1, arrival_time=3,
                          trip_id="tripI", seq=1, is_walk=False)
        journey1 = ForwardJourney(legs=[leg1])
        journey2 = ForwardJourney(legs=[leg2])
        journey12 = ForwardJourney(legs=[leg1, leg2])
        journey13 = ForwardJourney(legs=[leg1, leg3])
        self.assertTrue(journey12.dominates(journey13))
        self.assertFalse(journey1.dominates(journey2))
        self.assertTrue(journey1.dominates(journey1, consider_time=False, consider_boardings=False))

    def test_basics(self):
        event_list_raw_data = [
            (0, 1, 0, 10, "trip_1", 1),
            (1, 100, 32, 36, "trip_5", 1),
            (100, 3, 36, 40, "trip_5", 2),
            (3, 4, 40, 41, "trip_4", 1),
            (4, 2, 44, 50, None, 1)
        ]
        legs = list(map(lambda el: Connection(*el), event_list_raw_data))
        test_journey = ForwardJourney(legs)

        self.assertIsInstance(test_journey, ForwardJourney)
        self.assertIsInstance(test_journey.get_legs(), list)

        self.assertEqual(test_journey.n_boardings, 3)
        self.assertEqual(test_journey.get_transfers(), 2)
        self.assertEqual(test_journey.get_travel_time(), 50)
        self.assertIsInstance(test_journey.get_waiting_times(), list)
        self.assertEqual(test_journey.get_total_waiting_time(), 22 + 0 + 3)
        self.assertEqual(len(test_journey.get_all_stops()), 6)

    def test_transfer_stop_pairs(self):
        event_list_raw_data = [
            (0, 1, 0, 10, "trip_1", 1),
            (1, 100, 32, 36, "trip_5", 1),
            (100, 3, 36, 40, "trip_5", 2),
            (3, 4, 40, 41, "trip_4", 1),
            (4, 2, 44, 50, None, 1),
            (10, 11, 52, 55, "trip_6", 1)
        ]
        legs = list(map(lambda el: Connection(*el), event_list_raw_data))
        test_journey = ForwardJourney(legs)

        transfer_stop_pairs = test_journey.get_transfer_stop_pairs()
        print(transfer_stop_pairs)
        self.assertEqual(len(transfer_stop_pairs), 3)
        self.assertEqual(transfer_stop_pairs[0][0], 1)
        self.assertEqual(transfer_stop_pairs[0][1], 1)
        self.assertEqual(transfer_stop_pairs[1][0], 3)
        self.assertEqual(transfer_stop_pairs[1][1], 3)
        self.assertEqual(transfer_stop_pairs[2][0], 2)
        self.assertEqual(transfer_stop_pairs[2][1], 10)

