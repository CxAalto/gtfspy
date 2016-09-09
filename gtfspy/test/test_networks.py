from __future__ import print_function

import os
import unittest

import networkx

from gtfspy.gtfs import GTFS
from gtfspy import networks
from gtfspy.route_types import WALK, BUS
from gtfspy.calc_transfers import calc_transfers


class ExtractsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs_source_dir = self.__class__.gtfs_source_dir
        self.gtfs = self.__class__.G

    def test_walk_network(self):
        calc_transfers(self.gtfs.conn, 10**6)
        walk_net = networks.walk_stop_to_stop_network(self.gtfs)
        self.assertGreater(len(walk_net.nodes()), 0)
        self.assertGreater(len(walk_net.edges()), 1)
        for form_node, to_node, data_dict in walk_net.edges(data=True):
            self.assertIn("d_great_circle", data_dict)
            self.assertGreater(data_dict["d_great_circle"], 0)
            self.assertIsNone(data_dict["d_shape"])  # for this test data set, there is no OSM mapping

    # def test_undirected_line_graph(self):
    #     line_net = networks.undirected_stop_to_stop_network_with_route_information(self.gtfs)
    #     self.assertGreater(len(line_net.nodes()), 0, "there should be at least some nodes")
    #     self.assertGreater(len(line_net.edges()), 0, "there should be at least some edges")
    #     for from_node, to_node, data in line_net.edges(data=True):
    #         self.assertIn("route_ids", data)
    #         self.assertIsInstance(from_node, int)
    #         self.assertIsInstance(to_node, int)
    #         self.assertIn(from_node, line_net.nodes())
    #         self.assertIn(to_node, line_net.nodes())
    #     for node, data in line_net.nodes(data=True):
    #         self.assertIn("lat", data)
    #         self.assertIn("lon", data)
    #         self.assertIsInstance(node, int)
    #
    # def test_aggregate_line_network(self):
    #     orig_net = networks.undirected_stop_to_stop_network_with_route_information(self.gtfs)
    #     aggregate_net = networks.aggregate_route_network(self.gtfs, 1000)
    #     self.assertGreater(len(orig_net.nodes()), len(aggregate_net.nodes()))
    #     self.assertTrue(isinstance(aggregate_net, networkx.Graph))
    #     for node in aggregate_net.nodes():
    #         for orig_node in node:
    #             self.assertIn(orig_node, orig_net.nodes())
    #     self.assertEquals(len(orig_net.nodes(), sum(map(lambda x: len(x), aggregate_net.nodes()))))
    #
    #     fake_aggregate_net = networks.aggregate_route_network(self.gtfs, 0)
    #     self.assertEquals(len(orig_net.nodes()), len(fake_aggregate_net.nodes()), "no nodes should be using 0 distance")

    def test_stop_to_stop_network_by_route_type(self):
        # test that distance works
        all_link_attributes = ["capacity_estimate", "duration_min", "duration_max",
                               "duration_median", "duration_avg", "n_vehicles", "route_types",
                               "distance_great_circle", "distance_shape",
                               "route_ids"]
        nxGraph = networks.route_type_stop_to_stop_network(self.gtfs,
                                                           BUS,
                                                           link_attributes=all_link_attributes)
        self.assertTrue(isinstance(nxGraph, networkx.DiGraph), type(nxGraph))
        nodes = nxGraph.nodes(data=True)
        self.assertGreater(len(nodes), 0)
        for node in nodes:
            node_attrs = node[1]
            node_id = node[0]
            self.assertTrue(isinstance(node_id, int))
            self.assertTrue("lat" in node_attrs)
            self.assertTrue("lon" in node_attrs)
            self.assertTrue("name" in node_attrs)
        edges = nxGraph.edges(data=True)
        self.assertGreater(len(edges), 0)
        from_I, to_I, linkData = edges[0]
        for link_attr in all_link_attributes:
            self.assertIn(link_attr, linkData)
            if "duration_" in link_attr:
                self.assertGreaterEqual(linkData[link_attr], 0)

        at_least_one_shape_distance = False
        for from_I, to_I, linkData in edges:
            ds = linkData['distance_shape']
            self.assertTrue(isinstance(ds, int) or (ds is None),
                            "distance_shape should be either int or None (in case shapes are not available)")
            if isinstance(ds, int):
                at_least_one_shape_distance = True
            self.assertLessEqual(linkData['duration_min'], linkData["duration_avg"])
            self.assertLessEqual(linkData['duration_avg'], linkData["duration_max"])
            self.assertLessEqual(linkData['duration_median'], linkData["duration_max"])
            self.assertGreaterEqual(linkData['duration_median'], linkData["duration_min"])
            self.assertTrue(isinstance(linkData['distance_great_circle'], float),
                            "straight line distance should always exist and be a float")
            self.assertGreater(linkData['distance_great_circle'],
                               0,
                               "straight line distance should be always greater than 0 (?)")
            n_veh = linkData["n_vehicles"]
            route_types = linkData["route_types"]
            route_types_sum = sum([count for route_type, count in route_types.iteritems()])
            self.assertTrue(n_veh, route_types_sum)
            route_ids = linkData["route_ids"]
            route_ids_sum = sum([count for route_type, count in route_ids.iteritems()])
            self.assertTrue(n_veh, route_ids_sum)

        self.assertTrue(at_least_one_shape_distance, "at least one shape distance should exist")




