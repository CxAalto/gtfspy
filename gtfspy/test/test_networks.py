from __future__ import print_function

import os
import unittest

import networkx

from gtfspy.gtfs import GTFS
from gtfspy import networks
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
            self.assertIsNone(data_dict["d_walk"])  # for this test data set, there is no OSM mapping

    def test_aggregate_line_graph(self):
        net_old = networks.undirected_line_network(self.gtfs)
        net = networks.aggregate_line_network(self.gtfs, 1000)
        self.assertGreater(len(net_old.nodes()), len(net.nodes()))
        self.assertTrue(isinstance(net, networkx.Graph))
        net = networks.aggregate_line_network(self.gtfs, 0)
        self.assertEquals(len(net_old.nodes()), len(net.nodes()), "no nodes should be using 0 distance")

    def test_directed_graph(self):
        # test that distance works
        all_link_attributes = ["duration_min", "duration_max", "duration_median",
                               "duration_avg", "n_vehicles", "route_types",
                               "distance_straight_line", "distance_shape",
                               "route_ids"]
        nxGraph = networks.directed_stop_to_stop_network(self.gtfs, link_attributes=all_link_attributes)
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
            self.assertTrue(link_attr in linkData, "no " + link_attr + " found")
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
            self.assertTrue(isinstance(linkData['distance_straight_line'], float),
                            "straight line distance should always exist and be a float")
            self.assertGreater(linkData['distance_straight_line'], 0, "straight line distance should be always greater than 0 (?)")
            n_veh = linkData["n_vehicles"]
            route_types = linkData["route_types"]
            route_types_sum = sum([count for route_type, count in route_types.iteritems()])
            self.assertTrue(n_veh, route_types_sum)
            route_ids = linkData["route_ids"]
            route_ids_sum = sum([count for route_type, count in route_ids.iteritems()])
            self.assertTrue(n_veh, route_ids_sum)

        # print self.gtfs.get_table("trips")
        # print "printing tables:\n\n"
        # print self.gtfs.get_table("stop_times")
        # print self.gtfs.get_table("shapes")
        # # print self.gtfs.get_table("stops")
        self.assertTrue(at_least_one_shape_distance, "at least one shape distance should exist")

    def test_undirected_line_graph(self):
        line_net = networks.undirected_line_network(self.gtfs)
        self.assertGreater(len(line_net.nodes()), 0, "there should be at least some nodes")
        self.assertGreater(len(line_net.edges()), 0, "there should be at least some edges")
        for from_node, to_node, data in line_net.edges(data=True):
            self.assertIn("route_ids", data)
            assert isinstance(from_node, int)
            assert isinstance(to_node, int)
            assert from_node in line_net.nodes()
            assert to_node in line_net.nodes()
        for node, data in line_net.nodes(data=True):
            assert "lat" in data
            assert "lon" in data
            assert isinstance(node, int)


