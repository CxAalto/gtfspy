import unittest

import networkx

from gtfspy.routing.connection_scan import ConnectionScan
from gtfspy.routing.connection import Connection


class ConnectionScanTest(unittest.TestCase):
    def setUp(self):
        event_list_raw_data = [
            (1, 2, 0, 10, "trip_1", 1),
            (1, 3, 1, 10, "trip_2", 1),
            (2, 3, 10, 11, "trip_1", 2),
            (3, 4, 11, 13, "trip_1", 3),
            (3, 6, 12, 14, "trip_3", 1),
        ]
        self.transit_connections = map(lambda el: Connection(*el), event_list_raw_data)
        self.walk_network = networkx.Graph()
        self.walk_network.add_edge(4, 5, d_walk=1000)
        self.walk_speed = 10
        self.source_stop = 1
        self.end_time = 20
        self.transfer_margin = 2
        self.start_time = 0 - self.transfer_margin

    def test_basics(self):
        """
        This test tests some basic features of the algorithm, such as:
        1. Transfer margins are respected.
        2. Stop labels are respected.
        3. Walk network is used properly.
        """
        csa = ConnectionScan(
            self.transit_connections,
            self.source_stop,
            self.start_time,
            self.end_time,
            self.transfer_margin,
            self.walk_network,
            self.walk_speed,
        )
        csa.run()
        arrival_times = csa.get_arrival_times()
        self.assertEqual(arrival_times[1], self.start_time)
        self.assertEqual(arrival_times[2], 10)
        self.assertEqual(arrival_times[3], 10)
        self.assertEqual(arrival_times[4], 13)
        self.assertEqual(arrival_times[5], 13 + 100)
        self.assertEqual(arrival_times[6], 14)
        self.assertEqual(arrival_times[7], float("inf"))
        self.assertGreater(csa.get_run_time(), 0)

    def test_change_starttime(self):
        start_time = 1 - self.transfer_margin
        csa = ConnectionScan(
            self.transit_connections,
            self.source_stop,
            start_time,
            self.end_time,
            self.transfer_margin,
            self.walk_network,
            self.walk_speed,
        )
        csa.run()
        arrival_times = csa.get_arrival_times()
        self.assertEqual(arrival_times[1], start_time)
        self.assertEqual(arrival_times[2], float("inf"))
        self.assertEqual(arrival_times[3], 10)
        self.assertEqual(arrival_times[4], float("inf"))
        self.assertEqual(arrival_times[5], float("inf"))
        self.assertEqual(arrival_times[6], 14)
        self.assertEqual(arrival_times[7], float("inf"))

    def test_change_endtime(self):
        end_time = 11
        csa = ConnectionScan(
            self.transit_connections,
            self.source_stop,
            self.start_time,
            end_time,
            self.transfer_margin,
            self.walk_network,
            self.walk_speed,
        )
        csa.run()
        arrival_times = csa.get_arrival_times()
        self.assertEqual(arrival_times[1], self.start_time)
        self.assertEqual(arrival_times[2], 10)
        self.assertEqual(arrival_times[3], 10)
        self.assertEqual(arrival_times[4], 13)
        self.assertEqual(arrival_times[5], 13 + 100)
        self.assertEqual(arrival_times[6], float("inf"))
        self.assertEqual(arrival_times[7], float("inf"))

    def test_starts_with_walk(self):
        end_time = 11
        event_list_raw_data = [(1, 2, 0, 10, "trip_1", 1)]
        transit_connections = map(lambda el: Connection(*el), event_list_raw_data)
        walk_network = networkx.Graph()
        walk_network.add_edge(1, 2, d_walk=10)
        walk_speed = 10
        source_stop = 1
        start_time = 0
        transfer_margin = 0
        csa = ConnectionScan(
            transit_connections,
            source_stop,
            start_time,
            end_time,
            transfer_margin,
            walk_network,
            walk_speed,
        )
        csa.run()
        arrival_times = csa.get_arrival_times()
        self.assertEqual(arrival_times[1], start_time)
        self.assertEqual(arrival_times[2], 1)
