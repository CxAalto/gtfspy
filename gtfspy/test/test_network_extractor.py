import os
from unittest import TestCase

import networkx as nx

from ..gtfs import GTFS
from ..network_extractor import NetworkExtractor

class NetworkExtractorTest(TestCase):

    @classmethod
    def setUpClass(cls):
        """ This method is run once before executing any tests"""
        cls.gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        cls.G = GTFS.from_directory_as_inmemory_db(cls.gtfs_source_dir)

    def setUp(self):
        """This method is run once before _each_ test method is executed"""
        self.gtfs_source_dir = self.__class__.gtfs_source_dir
        self.gtfs = self.__class__.G
        self.exctractor = NetworkExtractor(self.gtfs)

    def test_stop_to_stop_network_format(self):
        directed_graph = self.exctractor.stop_to_stop_network()
        self.assertTrue(isinstance(directed_graph, nx.DiGraph))
        self.assertGreater(len(directed_graph.nodes()), 0)
        for node, data in directed_graph.nodes(data=True):
            self.assertTrue(isinstance(node, int))
            keys = "lat lon name".split()
            for key in keys:
                self.assertTrue(key in data)
                value = data[key]
                if key in ["lat", "lon"]:
                    self.assertTrue(isinstance(value, float))
                if key == "name":
                    self.assertTrue(isinstance(value, unicode))
        self.assertGreater(len(directed_graph.edges()), 0)
        for node, neighbor, data in directed_graph.edges(data=True):
            self.assertTrue(node in directed_graph)
            self.assertTrue(neighbor in directed_graph)
            keys = ["distance", "time", "n_vehicles", "capacity_per_hour", "lines", "modes"]
            for key in keys:
                self.assertTrue(key in data)
                value = data[key]
                if key in ["distance", "time", "n_vehicles", "capacity_per_hour"]:
                    self.assertTrue(isinstance(value, (int, float)))
                if key in ["lines"]:
                    self.assertTrue(isinstance(value, list))
                    for line in value:
                        self.assertTrue(isinstance(line, unicode))


