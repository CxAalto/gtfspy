from unittest import TestCase

import networkx

from gtfspy.routing.pseudo_connections import compute_pseudo_connections
from gtfspy.routing.models import Connection


class TestComputePseudoConnections(TestCase):

    def test_simple(self):
        event_list_raw_data = [
            (1, 2, 40, 50, "trip_5"),
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(2, 3, {"d_walk": 15})
        pseudo_connections = compute_pseudo_connections(transit_connections, 0, 120, 0, walk_network, 1)
        self.assertEqual(1, len(pseudo_connections))
        self.assertEqual(pseudo_connections[0], Connection(2, 3, 50, 65, None, True))
        self.assertTrue(pseudo_connections[0].is_walk)

    def test_no_pseudo_connection_if_outside_time_range(self):
        event_list_raw_data = [
            (1, 2, 40, 50, "trip_5"),
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(2, 3, {"d_walk": 15})
        pseudo_connections = compute_pseudo_connections(transit_connections, 0, 45, 0, walk_network, 1)
        self.assertEqual(0, len(pseudo_connections))

    def test_no_multiple_pseudo_connections(self):
        event_list_raw_data = [
            (1, 2, 40, 50, "trip_5"),
            (0, 2, 35, 50, "trip_6"),
        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(2, 3, {"d_walk": 15})
        pseudo_connections = compute_pseudo_connections(transit_connections, 0, 50, 0, walk_network, 1)
        self.assertEqual(1, len(pseudo_connections))




