from __future__ import print_function

import os
import unittest
import shutil

import networkx
import pandas

from gtfspy.gtfs import GTFS
from gtfspy import networks
from gtfspy.route_types import BUS
from gtfspy.calc_transfers import calc_transfers


class NetworkExtractsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    # noinspection PyUnresolvedReferences
    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs_source_dir = self.__class__.gtfs_source_dir
        self.gtfs = self.__class__.G
        self.extract_output_dir = os.path.join(self.gtfs_source_dir, "../", "test_gtfspy_extracts_8211231/")
        if os.path.exists(self.extract_output_dir):
            shutil.rmtree(self.extract_output_dir)

    def test_walk_network(self):
        calc_transfers(self.gtfs.conn, 10**6)
        walk_net = networks.walk_stop_to_stop_network(self.gtfs)
        self.assertGreater(len(walk_net.nodes()), 0)
        self.assertGreater(len(walk_net.edges()), 1)
        for form_node, to_node, data_dict in walk_net.edges(data=True):
            self.assertIn("d_great_circle", data_dict)
            self.assertGreater(data_dict["d_great_circle"], 0)
            self.assertIsNone(data_dict["d_shape"])  # for this test data set, there is no OSM routing done

    def test_write_stop_to_stop_networks(self):
        networks.write_stop_to_stop_networks(self.gtfs, self.extract_output_dir)
        self.assertTrue(os.path.exists(os.path.join(self.extract_output_dir + "walk.edg")))
        self.assertTrue(os.path.exists(os.path.join(self.extract_output_dir + "bus.edg")))

    def test_write_combined_stop_to_stop_networks(self):
        networks.write_combined_transit_stop_to_stop_network(self.gtfs, self.extract_output_dir)
        combined_file_name = os.path.join(self.extract_output_dir + "combined.edg")
        self.assertTrue(os.path.exists(combined_file_name))

    def test_stop_to_stop_network_by_route_type(self):
        # test that distance works
        nxGraph = networks.stop_to_stop_network_for_route_type(self.gtfs, BUS)
        self.assertTrue(isinstance(nxGraph, networkx.DiGraph), type(nxGraph))
        nodes = nxGraph.nodes(data=True)
        self.assertGreater(len(nodes), 0)
        for node in nodes:
            node_attributes = node[1]
            node_id = node[0]
            self.assertTrue(isinstance(node_id, int))
            self.assertTrue("lat" in node_attributes)
            self.assertTrue("lon" in node_attributes)
            self.assertTrue("name" in node_attributes)
        edges = nxGraph.edges(data=True)
        self.assertGreater(len(edges), 0)

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
            route_ids = linkData["route_ids"]
            route_ids_sum = sum([count for route_type, count in route_ids.iteritems()])
            self.assertTrue(n_veh, route_ids_sum)

        self.assertTrue(at_least_one_shape_distance, "at least one shape distance should exist")

    def test_combined_stop_to_stop_transit_network(self):
        multi_di_graph = networks.combined_stop_to_stop_transit_network(self.gtfs)
        self.assertIsInstance(multi_di_graph, networkx.MultiDiGraph)
        for from_node, to_node, data in multi_di_graph.edges(data=True):
            self.assertIn("route_type", data)

    def test_temporal_network(self):
        temporal_pd = networks.temporal_network(self.gtfs)
        self.assertGreater(temporal_pd.shape[0], 10)

    def test_write_temporal_network(self):
        path = os.path.join(self.extract_output_dir, "combined.tnet")
        networks.write_temporal_network(self.gtfs, path, None, None)
        self.assertTrue(os.path.exists(path))
        df = pandas.read_csv(path)
        columns_should_exist = ["dep_time_ut", "arr_time_ut", "from", "to", "route_type", "route_id", "trip_I"]
        for col in columns_should_exist:
            self.assertIn(col, df.columns.values)
        print(df)

    def test_write_temporal_networks_by_route_type(self):
        networks.write_temporal_networks_by_route_type(self.gtfs, self.extract_output_dir)
        self.assertTrue(os.path.exists(os.path.join(self.extract_output_dir + "bus.tnet")))


    # def test_clustered_stops_network(self):
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
    #     self.assertEquals(len(orig_net.nodes()), len(fake_aggregate_net.nodes()),
    #                       "no nodes should be using 0 distance")
