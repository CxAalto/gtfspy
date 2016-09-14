import unittest

import networkx

from gtfspy.routing.connection_scan import ConnectionScan, Connection


class ConnectionScanTest(unittest.TestCase):

    def setUp(self):
        event_list_raw_data = [
            (1, 2, 0, 10, "trip_1"),
            (1, 3, 1, 10, "trip_2"),
            (2, 3, 10, 11, "trip_1"),
            (3, 4, 11, 13, "trip_1"),
            (3, 6, 12, 14, "trip_3")
        ]
        self.transit_connections = map(lambda el: Connection(*el), event_list_raw_data)
        self.walk_network = networkx.Graph()
        self.walk_network.add_edge(4, 5, {"distance_shape": 1000})
        self.walk_speed = 10

    def test_basics(self):
        """
        This test tests some basic features of the algorithm, such as:
        1. Transfer margins are respected.
        2. Stop labels are respected.
        3. Walk network is used properly.
        """
        seed_stop = 1
        end_time = 20
        transfer_margin = 2
        start_time = 0 - transfer_margin
        csa = ConnectionScan(self.transit_connections, seed_stop, start_time, end_time,
                             transfer_margin, self.walk_network, self.walk_speed)
        csa.run()
        arrival_times = csa.get_arrival_times()
        self.assertEqual(arrival_times[1], -2)
        self.assertEqual(arrival_times[2], 10)
        self.assertEqual(arrival_times[3], 10)
        self.assertEqual(arrival_times[4], 13)
        self.assertEqual(arrival_times[5], 13 + 100)
        self.assertEqual(arrival_times[6], 14)
        self.assertEqual(arrival_times[7], float('inf'))



