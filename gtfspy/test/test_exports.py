import io
import os
import unittest
import shutil

import geopandas
import networkx
import numpy
import pandas
import geojson

from gtfspy.gtfs import GTFS
from gtfspy import networks
from gtfspy.networks import ALL_STOP_TO_STOP_LINK_ATTRIBUTES
from gtfspy.route_types import BUS
from gtfspy import exports
from gtfspy.util import makedirs


class ExportsTest(unittest.TestCase):

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
        if not os.path.exists(self.extract_output_dir):
            makedirs(self.extract_output_dir)

    def tearDown(self):
        if os.path.exists(self.extract_output_dir):
            shutil.rmtree(self.extract_output_dir)

    def test_walk_network(self):
        walk_net = networks.walk_transfer_stop_to_stop_network(self.gtfs)
        self.assertGreater(len(walk_net.nodes()), 0)
        self.assertGreater(len(walk_net.edges()), 1)
        for form_node, to_node, data_dict in walk_net.edges(data=True):
            self.assertIn("d", data_dict)
            self.assertGreaterEqual(data_dict["d"], 0)
        threshold = 670
        walk_net = networks.walk_transfer_stop_to_stop_network(self.gtfs, max_link_distance=threshold)
        self.assertEqual(len(walk_net.edges()), 2)
        for form_node, to_node, data_dict in walk_net.edges(data=True):
            self.assertLess(data_dict['d'], threshold)

    def test_write_stop_to_stop_networks(self):
        exports.write_static_networks(self.gtfs, self.extract_output_dir)
        self.assertTrue(os.path.exists(self.extract_output_dir + "network_walk.edg"))
        self.assertTrue(os.path.exists(self.extract_output_dir + "network_bus.edg"))
        self.assertFalse(os.path.exists(self.extract_output_dir + "network_gondola.edg"))

    def test_write_combined_stop_to_stop_networks(self):
        output = os.path.join(self.extract_output_dir + "network_combined.edg")
        exports.write_combined_transit_stop_to_stop_network(self.gtfs, output)
        self.assertTrue(os.path.exists(output))

    def test_stop_to_stop_network_by_route_type(self):
        # test that distance works
        nxGraph = networks.stop_to_stop_network_for_route_type(self.gtfs, BUS,
                                                               link_attributes=ALL_STOP_TO_STOP_LINK_ATTRIBUTES)
        self.assertTrue(isinstance(nxGraph, networkx.DiGraph), type(nxGraph))
        nodes = nxGraph.nodes(data=True)
        self.assertGreater(len(nodes), 0)
        for node in nodes:
            node_attributes = node[1]
            node_id = node[0]
            self.assertTrue(isinstance(node_id, (int, numpy.int_)))
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
            self.assertTrue(isinstance(linkData['d'], int),
                            "straight line distance should always exist and be an int")
            self.assertGreaterEqual(linkData['d'],
                               0,
                               "straight line distance should be always greater than or equal to 0 (?)")
            n_veh = linkData["n_vehicles"]
            route_ids = linkData["route_I_counts"]
            route_ids_sum = sum([count for route_type, count in route_ids.items()])
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
        exports.write_temporal_network(self.gtfs, path, None, None)
        self.assertTrue(os.path.exists(path))
        df = pandas.read_csv(path)
        columns_should_exist = ["dep_time_ut", "arr_time_ut", "from_stop_I", "to_stop_I",
                                "route_type", "trip_I"]
        for col in columns_should_exist:
            self.assertIn(col, df.columns.values)

    def test_write_temporal_networks_by_route_type(self):
        exports.write_temporal_networks_by_route_type(self.gtfs, self.extract_output_dir)
        self.assertTrue(os.path.exists(os.path.join(self.extract_output_dir + "bus.tnet")))

    def test_write_gtfs_agencies(self):
        required_columns = 'agency_id,agency_name,agency_url,agency_timezone,agency_phone,agency_lang'.split(",")
        optional_columns = ['agency_lang', 'agency_phone', 'agency_fare_url', 'agency_email']
        self.__test_write_gtfs_table(exports._write_gtfs_agencies, required_columns, optional_columns)

    def test_write_gtfs_stops(self):
        required_columns = 'stop_id,stop_name,stop_desc,stop_lat,stop_lon'.split(",")
        optional_columns = ['stop_code', 'stop_desc', 'zone_id', 'stop_url', 'location_type', 'parent_station',
                               'stop_timezone', 'wheelchair_boarding']
        self.__test_write_gtfs_table(exports._write_gtfs_stops, required_columns, optional_columns)

    def test_write_gtfs_routes(self):
        required_columns = 'route_id,agency_id,route_short_name,route_long_name,route_desc,route_type'.split(",")
        optional_columns = ['route_desc', 'route_url', 'route_color', 'route_text_color']
        self.__test_write_gtfs_table(exports._write_gtfs_routes, required_columns, optional_columns)

    def test_write_gtfs_trips(self):
        required_columns = 'route_id,service_id,trip_id'.split(",")
        optional_columns = ['trip_headsign', 'trip_short_name', 'direction_id', 'block_id',
                                'shape_id', 'wheelchair_accessible', 'bikes_allowed']
        self.__test_write_gtfs_table(exports._write_gtfs_trips, required_columns, optional_columns)

    def test_write_gtfs_stop_times(self):
        required_columns = 'trip_id,arrival_time,departure_time,stop_id,stop_sequence'.split(",")
        optional_columns = ['stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled', 'timepoint']
        self.__test_write_gtfs_table(exports._write_gtfs_stop_times, required_columns, optional_columns)

    def test_write_gtfs_calendar(self):
        required_columns = 'service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,' \
                            'start_date,end_date'.split(",")
        self.__test_write_gtfs_table(exports._write_gtfs_calendar, required_columns, [])
        in_memory_file = io.StringIO()
        exports._write_gtfs_calendar(self.gtfs, in_memory_file)
        in_memory_file.seek(0)
        df = pandas.read_csv(in_memory_file)
        self.assertTrue("-" not in str(df['start_date'][0]))
        self.assertTrue("-" not in str(df['end_date'][0]))
        self.assertTrue(len(str(df['start_date'][0])) == 8)
        self.assertTrue(len(str(df['start_date'][0])) == 8)

    def test_write_gtfs_calendar_dates(self):
        required_columns = 'service_id,date,exception_type'.split(",")
        self.__test_write_gtfs_table(exports._write_gtfs_calendar_dates, required_columns, [])

    def test_write_gtfs_shapes(self):
        required_columns = 'shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence'.split(",")
        optional_columns = ['shape_dist_traveled']
        self.__test_write_gtfs_table(exports._write_gtfs_shapes, required_columns, optional_columns)

    def test_write_gtfs_transfers(self):
        required_columns = 'from_stop_id,to_stop_id,transfer_type'.split(",")
        optional_columns = ['min_transfer_time']
        self.__test_write_gtfs_table(exports._write_gtfs_transfers, required_columns, optional_columns)

    def test_write_gtfs_stop_distances(self):
        required_columns = 'from_stop_id,to_stop_id,d,d_walk'.split(",")
        optional_columns = []
        self.__test_write_gtfs_table(exports._write_gtfs_stop_distances, required_columns, optional_columns)

    def test_write_feed_info(self):
        required_columns = 'feed_publisher_name,feed_publisher_url,feed_lang'.split(",")
        columns_not_present = ['feed_start_date', 'feed_end_date', 'feed_version', 'feed_id']
        self.__test_write_gtfs_table(exports._write_gtfs_feed_info, required_columns, columns_not_present)

    def __test_write_gtfs_table(self, table_write_func, required_columns, optional_columns):
        """
        A helper method for testing writing of gtfs_table methods

        Parameters
        ----------
        table_write_func: function
        required_columns: list[str]
        optional_columns: list[str]
        """
        all_columns = set(required_columns)
        all_columns.update(set(optional_columns))

        in_memory_file = io.StringIO()
        table_write_func(self.gtfs, in_memory_file)
        in_memory_file.seek(0)
        header_columns = in_memory_file.readline().strip().split(",")
        for required_col in required_columns:
            self.assertIn(required_col, header_columns)
        for header_col in header_columns:
            self.assertIn(header_col, all_columns)

    def test_write_gtfs(self):
        # A simple import-output-import test"
        for ending in ["", ".zip"]:
            from gtfspy.import_gtfs import import_gtfs
            UUID = "36167f3012fe11e793ae92361f002671"
            sqlite_fname = "test_" + UUID + ".sqlite"
            test_output_dir = "./test_output_dir_" + UUID
            try:
                shutil.rmtree(test_output_dir)
            except FileNotFoundError:
                pass

            try:
                exports.write_gtfs(self.gtfs, test_output_dir + ending)
                self.assertTrue(os.path.exists(test_output_dir + ending))
                try:
                    G = import_gtfs(test_output_dir + ending, os.path.join(sqlite_fname))
                    self.assertTrue(os.path.exists(sqlite_fname))
                finally:
                    os.remove(sqlite_fname)
            finally:
                if ending == "":
                    shutil.rmtree(test_output_dir + ending)
                else:
                    os.remove(test_output_dir + ending)

    def test_write_stops_geojson(self):
        in_memory_file = io.StringIO()
        exports.write_stops_geojson(self.gtfs, in_memory_file)
        in_memory_file.seek(0)
        self.assertTrue(geojson.is_valid(in_memory_file.read(-1)))
        in_memory_file.seek(0)
        gjson = geojson.loads(in_memory_file.read(-1))
        gjson_properties = gjson['features'][0]['properties']
        self.assertIn("name", gjson_properties.keys())
        self.assertIn("stop_I", gjson_properties.keys())

    def test_write_sections_geojson(self):
        in_memory_file = io.StringIO()
        exports.write_sections_geojson(self.gtfs, in_memory_file)
        in_memory_file.seek(0)
        self.assertTrue(geojson.is_valid(in_memory_file.read(-1)))
        in_memory_file.seek(0)
        gjson = geojson.loads(in_memory_file.read(-1))
        gjson_properties = gjson['features'][0]['properties']
        self.assertIn("from_stop_I", gjson_properties.keys())
        self.assertIn("to_stop_I", gjson_properties.keys())
        self.assertIn("n_vehicles", gjson_properties.keys())
        self.assertIn("duration_avg", gjson_properties.keys())
        self.assertIn("route_I_counts", gjson_properties.keys())
        self.assertIn("route_type", gjson_properties.keys())

    def test_write_routes_geojson(self):
        in_memory_file = io.StringIO()
        exports.write_routes_geojson(self.gtfs, in_memory_file)
        in_memory_file.seek(0)
        self.assertTrue(geojson.is_valid(in_memory_file.read(-1)))
        in_memory_file.seek(0)
        gjson = geojson.loads(in_memory_file.read(-1))
        gjson_properties = gjson['features'][0]['properties']
        self.assertIn("route_type", gjson_properties.keys())
        self.assertIn("route_I", gjson_properties.keys())
        self.assertIn("route_name", gjson_properties.keys())






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
