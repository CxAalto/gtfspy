import logging
import warnings
from collections import Counter, defaultdict
import numpy
import os
import pandas as pd
import sqlite3
import sys
import time
import datetime

import shutil

import networkx

import pytz
import calendar

import db
import shapes
import util
from spreading_util import SpreadingStop
from spreading_util import Heap
from spreading_util import Event
from util import wgs84_distance

# py2/3 compatibility (copied from six)
if sys.version_info[0] == 3:
    binary_type = bytes
else:
    binary_type = str

# Setting up travel modes, directly extending from GTFS specification, see:
# https://developers.google.com/transit/gtfs/reference#routestxt
TRAVEL_MODE_WALK = -1
TRAVEL_MODE_TRAM = 0
TRAVEL_MODE_SUBWAY = 1
TRAVEL_MODE_RAIL = 2
TRAVEL_MODE_BUS = 3
TRAVEL_MODE_FERRY = 4
TRAVEL_MODE_CABLE_CAR = 5
TRAVEL_MODE_GONDOLA = 6
TRAVEL_MODE_FUNICULAR = 7

TRAVEL_MODES = {
    TRAVEL_MODE_WALK: "Walking layer",
    TRAVEL_MODE_TRAM: "Tram, Streetcar, Light rail. Any light rail "
                       "or street level system within a metropolitan area.",
    TRAVEL_MODE_SUBWAY: "Subway, Metro. Any underground rail system within a metropolitan area.",
    TRAVEL_MODE_RAIL: "Rail. Used for intercity or long - distance travel.",
    TRAVEL_MODE_BUS: "Bus. Used for short- and long-distance bus routes.",
    TRAVEL_MODE_FERRY: "Ferry. Used for short- and long-distance boat service.",
    TRAVEL_MODE_CABLE_CAR: "Cable car. Used for street-level cable cars "
                            "where the cable runs beneath the car.",
    TRAVEL_MODE_GONDOLA: "Gondola, Suspended cable car. "
                          "Typically used for aerial cable cars where "
                          "the car is suspended from the cable.",
    TRAVEL_MODE_FUNICULAR: "Funicular. Any rail system designed for steep inclines."
}


class GTFS(object):

    def __init__(self, fname):
        """Open a GTFS object

        Parameters
        ----------
        fname: str, sqlite3.connection object
            path to the
        """
        if isinstance(fname, (str, unicode)):
            self.conn = sqlite3.connect(fname)
            self.fname = fname
            # memory-mapped IO size, in bytes
            self.conn.execute('PRAGMA mmap_size = 1000000000;')
            # page cache size, in negative KiB.
            self.conn.execute('PRAGMA cache_size = -2000000;')

        elif isinstance(fname, sqlite3.Connection):
            self.conn = fname
            self._dont_close = True
        else:
            raise NotImplementedError("Initiating GTFS using an object with type " + str(type(fname))
                                      + " is not supported")

        # Set timezones
        self._timezone = pytz.timezone(self.get_timezone_name())
        self.meta = GTFSMetadata(self.conn)
        # Bind functions
        from util import wgs84_distance
        self.conn.create_function("find_distance", 4, wgs84_distance)


    def __del__(self):
        if not getattr(self, '_dont_close', False):
            self.conn.close()

    @classmethod
    def from_directory_as_inmemory_db(cls, gtfs_directory):
        """
        Instantiate a GTFS object by computing

        Parameters
        ----------
        gtfs_directory: str
            path to the directory for importing the database
        """
        import import_gtfs
        conn = sqlite3.connect(":memory:")
        import_gtfs.import_gtfs(gtfs_directory,
                                conn,
                                preserve_connection=True,
                                print_progress=False)
        return cls(conn)

    def get_main_database_path(self):
        """
        Should return the path to the database

        Returns
        -------
        path : str
            path to the database, empty string for in-memory databases
        """
        cur = self.conn.cursor()
        cur.execute("PRAGMA database_list")
        rows = cur.fetchall()
        for row in rows:
            print row
            if row[1] == str("main"):
                return row[2]

    def to_directed_graph(self,
                          link_attributes=None,
                          start_time_ut=None,
                          end_time_ut=None
                          # node_attributes=None
                          ):
        """
        Get a static graph presentation (networkx graph) of the GTFS feed.
        Node indices correspond to integers (stop_I's in the underlying GTFS database).

        Parameters
        ----------
        link_attributes : list, optional
            What link attributes should be computed
                "n_vehicles" : Number of vehicles passed
                "route_types" : GTFS route types that take place within the (i.e. which layer are we talking about)
                "duration_min" : minimum travel time between stops
                "duration_max" : maximum travel time between stops
                "duration_median" : median travel time between stops
                "duration_avg" : average travel time between stops
                "distance_straight_line" : distance along straight line (wgs84_distance)
                "distance_shape" : minimum distance along shape
                "capacity_estimate"  : approximate capacity passed
                "route_ids" : route id
        start_time_ut : int
            only events taking place after this moment are taken into account
        end_time_ut : int
            only events taking place before this moment are taken into account
        # node_attributes : None
        #     Defaulting to include all the following: latitude, longitude and stop_name
        #         "lat" : float, lon coordinate
        #         "lon" : float, lat coordinate
        #         "name" : str, name of the stop
        # directed : bool
        #    Is the network directed or not.
        #    In case of (undirected) links (i.e. walking), then links to both directions are included.
        # travel_modes : list, or set, optional (defaulting to all)
        #    A collection of integers as specified by GTFS:
        #    https://developers.google.com/transit/gtfs/reference#routestxt

        Returns
        -------
        net : an instance of the networkx graphs
            networkx.DiGraph : undirected simple
        """
        # get all nodes and their attributes
        net = networkx.DiGraph()
        nodeDataFrame = self.get_stop_info()  # node data frame
        for stopTuple in nodeDataFrame.itertuples(index=False, name="NamedTupleStop"):
            node_attributes = {
                "lat": stopTuple.lat,
                "lon": stopTuple.lon,
                "name": stopTuple.name,
            }
            net.add_node(stopTuple.stop_I, attr_dict=node_attributes)
        n_nodes = len(net.nodes())

        # get all trips
        events_df = self.get_transit_events(start_time_ut=start_time_ut, end_time_ut=end_time_ut)
        print events_df.columns.values

        # group events by links, and loop over them (i.e. each link):
        link_event_groups = events_df.groupby(['from_stop_I', 'to_stop_I'], sort=False)
        for key, link_events in link_event_groups:
            from_stop_I, to_stop_I = key
            assert isinstance(link_events, pd.DataFrame)
            # link_events columns:
            # 'dep_time_ut' 'arr_time_ut' 'shape_id' 'route_type' 'trip_I' 'duration' 'from_seq' 'to_seq'
            if link_attributes is None:
                net.add_edge(from_stop_I, to_stop_I)
            else:
                link_data = {}
                if "duration_min" in link_attributes:
                    link_data['duration_min'] = link_events['duration'].min()
                if "duration_max" in link_attributes:
                    link_data['duration_max'] = link_events['duration'].max()
                if "duration_median" in link_attributes:
                    link_data['duration_median'] = link_events['duration'].median()
                if "duration_avg" in link_attributes:
                    link_data['duration_avg'] = link_events['duration'].mean()
                # statistics on numbers of vehicles:
                if "n_vehicles" in link_attributes:
                    link_data['n_vehicles'] = link_events.shape[0]
                if "route_types" in link_attributes:
                    link_data['route_types'] = link_events.groupby('route_type').size().to_dict()
                if "capacity_estimate" in link_attributes:
                    #TODO !
                    raise NotImplementedError
                if "distance_straight_line" in link_attributes:
                    from_lat = net.node[from_stop_I]['lat']
                    from_lon = net.node[from_stop_I]['lon']
                    to_lat = net.node[to_stop_I]['lat']
                    to_lon = net.node[to_stop_I]['lon']
                    distance = wgs84_distance(from_lat, from_lon, to_lat, to_lon)
                    link_data['distance_straight_line'] = distance
                if "distance_shape" in link_attributes:
                    found = None
                    assert "shape_id" in link_events.columns.values
                    for i, shape_id in enumerate(link_events["shape_id"].values):
                        if shape_id is not None:
                            found = i
                            break
                    if found is None:
                        link_data["distance_shape"] = None
                    else:
                        link_event = link_events.iloc[found]
                        distance = self.get_shape_distance_between_stops(
                            link_event["trip_I"],
                            int(link_event["from_seq"]),
                            int(link_event["to_seq"])
                        )
                        link_data['distance_shape'] = distance
                if "route_ids" in link_attributes:
                    link_data["route_ids"] = link_events.groupby("route_id").size().to_dict()
                net.add_edge(from_stop_I, to_stop_I, attr_dict=link_data)

        assert len(net.nodes()) == n_nodes

        return net

    def to_undirected_line_graph(self, verbose=True):
        """
        Return a graph, where edges have route_id as labels. Only one arbitrary "instance/trip" of each "route"
        (or "line") is taken into account.

        Returns
        -------
        giant : the largest connected component of the undirected line graph (non-connected stops are filtered out)

        """
        net = networkx.Graph()
        nodeDataFrame = self.get_stop_info()  # node data frame
        for stopTuple in nodeDataFrame.itertuples(index=False, name="NamedTupleStop"):
            node_attributes = {
                "lat": stopTuple.lat,
                "lon": stopTuple.lon,
                "name": stopTuple.name,
            }
            net.add_node(stopTuple.stop_I, attr_dict=node_attributes)

        rows = self.conn.cursor().execute(
                                        "SELECT trip_I, route_I, route_id "
                                        "FROM routes "
                                        "LEFT JOIN trips "
                                        "USING(route_I) "
                                        "GROUP BY route_I").fetchall()
                                        # to take only one route per line
                                        # as looping over all trip_Is is too costly

        for trip_I, route_I, route_id in rows:
            if trip_I is None:
                continue
            query2 = "SELECT stop_I, seq " \
                     "FROM stop_times " \
                     "WHERE trip_I={trip_I} " \
                     "ORDER BY seq".format(trip_I=trip_I)
            df = pd.read_sql(query2, self.conn)
            stop_Is = df['stop_I'].values
            edges = zip(stop_Is[:-1], stop_Is[1:])
            for from_stop_I, to_stop_I in edges:
                if net.has_edge(from_stop_I, to_stop_I):
                    edge_data = net.get_edge_data(from_stop_I, to_stop_I)
                    edge_data["route_ids"].append(route_id)
                    edge_data["route_Is"].append(route_I)
                else:
                    net.add_edge(from_stop_I, to_stop_I,
                                 route_ids=[route_id], route_Is=[route_I])
        if verbose:
            if len(net.edges()) == 0:
                print "Warning: no edges in the line network, is the stop_times table defined properly?"

        # return only the maximum connected component to remove unassosicated nodes
        giant = max(networkx.connected_component_subgraphs(net), key=len)
        return giant

    def undir_line_graph_to_aggregated(self, graph, distance):
        """
        See to_aggregate_line_graph for documentation
        """
        assert distance <= 1000, "only works with distances below 1000 meters"
        nodes = set(graph.nodes())

        node_distance_graph = networkx.Graph()

        stop_distances = self.get_table("stop_distances")
        stop_pairs = stop_distances[stop_distances['d'] <= distance]
        stop_pairs = zip(stop_pairs['from_stop_I'], stop_pairs['to_stop_I'])
        for node in nodes:
            node_distance_graph.add_node(node)
        for node, another_node in stop_pairs:
            if (node in nodes) and (another_node in nodes):
                node_distance_graph.add_edge(node, another_node)

        node_group_iter = networkx.connected_components(node_distance_graph)

        aggregate_graph = networkx.Graph()
        old_node_to_new_node = {}
        for node_group in node_group_iter:
            new_node_id = tuple(node for node in node_group)
            lats = []
            lons = []
            names = []
            for node in node_group:
                if node not in graph:
                    # some stops may not part of the original node line graph
                    # (e.g. if some lines are not considered, or there are extra stops in stops table)
                    continue
                old_node_to_new_node[node] = new_node_id
                lats.append(graph.node[node]['lat'])
                lons.append(graph.node[node]['lon'])
                names.append(graph.node[node]['name'])
            new_lat = numpy.mean(lats)
            new_lon = numpy.mean(lons)
            attr_dict = {
                "lat"  : new_lat,
                "lon"  : new_lon,
                "names": names
            }
            aggregate_graph.add_node(new_node_id, attr_dict=attr_dict)

        for from_node, to_node, data in graph.edges(data=True):
            new_from_node = old_node_to_new_node[from_node]
            new_to_node = old_node_to_new_node[to_node]
            if aggregate_graph.has_edge(new_from_node, new_to_node):
                edge_data = aggregate_graph.get_edge_data(new_from_node, new_to_node)
                edge_data['route_ids'].append(data['route_ids'])
            else:
                aggregate_graph.add_edge(new_from_node, new_to_node, route_ids=data['route_ids'])

        return aggregate_graph

    def to_aggregate_line_graph(self, distance):
        """
        Aggregate graph by grouping nodes that are within
        a specified distance.
        The new node_ids are tuple of the old node_ids.

        Parameters
        ----------
        distance : float
            group all nodes within this distance.

        Returns
        -------
        graph : networkx.Graph
        """
        graph = self.to_undirected_line_graph()
        # 1st group the nodes
        return self.undir_line_graph_to_aggregated(graph, distance)

    def get_shape_distance_between_stops(self, trip_I, from_stop_seq, to_stop_seq):
        """
        Get the distance along a shape between stops

        Parameters
        ----------
        trip_I : int
            trip_ID along which we travel
        from_stop_seq : int
            the sequence number of the 'origin' stop
        to_stop_seq : int
            the sequence number of the 'destination' stop

        Returns
        -------
        distance : float, None
            If the shape calculation succeeded, return a float, otherwise return None
            (i.e. in the case where the shapes table is empty)
        """
        query_template = "SELECT shape_break FROM stop_times WHERE trip_I={trip_I} AND seq={seq} "
        stop_seqs = [from_stop_seq, to_stop_seq]
        shape_breaks = []
        for seq in stop_seqs:
            q = query_template.format(seq=seq, trip_I=trip_I)
            shape_breaks.append(self.conn.execute(q).fetchone())
        query_template = "SELECT max(d) - min(d) " \
                         "FROM shapes JOIN trips ON(trips.shape_id=shapes.shape_id) " \
                         "WHERE trip_I={trip_I} AND shapes.seq>={from_stop_seq} AND shapes.seq<={to_stop_seq};"
        distance_query = query_template.format(trip_I=trip_I, from_stop_seq=from_stop_seq, to_stop_seq=to_stop_seq)
        return self.conn.execute(distance_query).fetchone()[0]

    def copy_and_filter(self,
                        copy_db_path,
                        buffer_distance=None,
                        buffer_lat=None,
                        buffer_lon=None,
                        update_metadata=True,
                        start_date=None,
                        end_date=None,
                        agency_ids_to_preserve=None,
                        agency_distance=None):
        """
        Copy a database, and then based on various filters.
        Only copy_and_filter method is provided as of now because we do not want to take the risk of
        losing any data of the original databases.

        copy_db_path : str
            path to another database database
        update_metadata : boolean, optional
            whether to update metadata of the feed, defaulting to true
            (this option is mainly available for testing purposes)
        start_date : unicode, or datetime.datetime
            filter out all data taking place before end_date (the start_time_ut of the end date)
            Date format "YYYY-MM-DD"
            (end_date_ut is not included after filtering)
        end_date : unicode, or datetime.datetime
            Filter out all data taking place after end_date
            The end_date is not included after filtering.
        agency_ids_to_preserve : iterable
            List of agency_ids to retain (str) (e.g. 'HSL' for Helsinki)
            Only routes by the listed agencies are then considered
        agency_distance : float
            Only evaluated in combination with agency filter.
            Distance (in km) to the other near-by stops that should be included in addition to
            the ones defined by the agencies.
            All vehicle trips going through at least two such stops would then be included in the
            export. Note that this should not be a recursive thing.
            Or should it be? :)
        buffer_lat : float
            Latitude of the buffer zone center
        buffer_lon : float
            Longitude of the buffer zone center
        buffer_distance : float
            Distance from the buffer zone center (in meters)

        Returns
        -------
        None
        """
        if agency_distance is not None:
            raise NotImplementedError
        this_db_path = self.get_main_database_path()
        assert os.path.exists(this_db_path), "Copying of in-memory databases is not supported"
        assert os.path.exists(os.path.dirname(os.path.abspath(copy_db_path))), \
            "the directory where the copied database will reside should exist beforehand"
        assert not os.path.exists(copy_db_path), "the resulting database exists already"

        # this with statement
        # is used to ensure that no corrupted/uncompleted files get created in case of problems
        with util.create_file(copy_db_path) as tempfile:
            logging.info("copying database")
            shutil.copy(this_db_path, tempfile)
            copy_db_conn = sqlite3.connect(tempfile)
            assert isinstance(copy_db_conn, sqlite3.Connection)

            # filter by start_time_ut and end_date_ut:
            if (start_date is not None) and (end_date is not None):
                logging.info("Filtering based on agency_ids")
                start_date_ut = self.get_day_start_ut(start_date)
                end_date_ut = self.get_day_start_ut(end_date)
                # negated from import_gtfs
                table_to_remove_map = {
                    "calendar": ("WHERE NOT ("
                                 "date({start_ut}, 'unixepoch', 'localtime') < end_date "
                                 "AND "
                                 "start_date < date({end_ut}, 'unixepoch', 'localtime')"
                                 ");"),
                    "calendar_dates": "WHERE NOT ("
                                      "date({start_ut}, 'unixepoch', 'localtime') <= date "
                                      "AND "
                                      "date < date({end_ut}, 'unixepoch', 'localtime')"
                                      ")",
                    "day_trips2": 'WHERE NOT ('
                                  '{start_ut} < end_time_ut '
                                  'AND '
                                  'start_time_ut < {end_ut}'
                                  ')',
                    "days": "WHERE NOT ("
                            "{start_ut} <= day_start_ut "
                            "AND "
                            "day_start_ut < {end_ut}"
                            ")"
                }
                # remove the 'source' entries from tables
                for table, query_template in table_to_remove_map.iteritems():
                    param_dict = {"start_ut": str(start_date_ut),
                                  "end_ut": str(end_date_ut)}
                    query = "DELETE FROM " + table + " " + \
                            query_template.format(**param_dict)
                    copy_db_conn.execute(query)

                # update calendar table's services
                if isinstance(start_date, (datetime.datetime, datetime.date)):
                    start_date = start_date.strftime("%Y-%m-%d")
                if not isinstance(end_date, (datetime.datetime, datetime.date)):
                    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                end_date_to_include = end_date - datetime.timedelta(days=1)
                end_date_to_include_str = end_date_to_include.strftime("%Y-%m-%d")

                start_date_query = "UPDATE calendar " \
                                   "SET start_date='{start_date}' " \
                                   "WHERE start_date<'{start_date}' ".format(
                    **{"start_date": start_date}
                )
                copy_db_conn.execute(start_date_query)

                end_date_query = "UPDATE calendar " \
                                 "SET end_date='{end_date_to_include}' " \
                                 "WHERE end_date>'{end_date_to_include}' ".format(
                    **{"end_date_to_include": end_date_to_include_str}
                )
                copy_db_conn.execute(end_date_query)

                # then recursively delete further data:
                copy_db_conn.execute('DELETE FROM trips WHERE '
                                     'trip_I NOT IN (SELECT trip_I FROM days)')
                copy_db_conn.execute('DELETE FROM shapes WHERE '
                                     'shape_id NOT IN (SELECT shape_id FROM trips)')
                copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                     'trip_I NOT IN (SELECT trip_I FROM trips)')
                copy_db_conn.execute('DELETE FROM stops WHERE '
                                     'stop_I NOT IN (SELECT stop_I FROM stop_times)')
                copy_db_conn.execute('DELETE FROM stops_rtree WHERE '
                                     'stop_I NOT IN (SELECT stop_I FROM stops)')
                copy_db_conn.execute('DELETE FROM stop_distances WHERE '
                                     '   from_stop_I NOT IN (SELECT stop_I FROM stops) '
                                     'OR to_stop_I   NOT IN (SELECT stop_I FROM stops)')
                copy_db_conn.execute('DELETE FROM routes WHERE '
                                     'route_I NOT IN (SELECT route_I FROM trips)')
                copy_db_conn.execute('DELETE FROM agencies WHERE '
                                     'agency_I NOT IN (SELECT agency_I FROM routes)')
                copy_db_conn.commit()

            # filter by agency ids
            if agency_ids_to_preserve is not None:
                logging.info("Filtering based on agency_ids")
                agency_ids_to_preserve = list(agency_ids_to_preserve)
                agencies = pd.read_sql("SELECT * FROM agencies", copy_db_conn)
                agencies_to_remove = []
                for idx, row in agencies.iterrows():
                    if row['agency_id'] not in agency_ids_to_preserve:
                        agencies_to_remove.append(row['agency_id'])
                for agid in agencies_to_remove:
                    copy_db_conn.execute('DELETE FROM agencies WHERE agency_id=?', (agid,))
                # and remove recursively related to the agencies:
                copy_db_conn.execute('DELETE FROM routes WHERE '
                                     'agency_I NOT IN (SELECT agency_I FROM agencies)')
                copy_db_conn.execute('DELETE FROM trips WHERE '
                                     'route_I NOT IN (SELECT route_I FROM routes)')
                copy_db_conn.execute('DELETE FROM calendar WHERE '
                                     'service_I NOT IN (SELECT service_I FROM trips)')
                copy_db_conn.execute('DELETE FROM calendar_dates WHERE '
                                     'service_I NOT IN (SELECT service_I FROM trips)')
                copy_db_conn.execute('DELETE FROM days WHERE '
                                     'trip_I NOT IN (SELECT trip_I FROM trips)')
                copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                     'trip_I NOT IN (SELECT trip_I FROM trips)')
                copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                     'trip_I NOT IN (SELECT trip_I FROM trips)')
                copy_db_conn.execute('DELETE FROM shapes WHERE '
                                     'shape_id NOT IN (SELECT shape_id FROM trips)')
                copy_db_conn.execute('DELETE FROM day_trips2 WHERE '
                                     'trip_I NOT IN (SELECT trip_I FROM trips)')
                copy_db_conn.commit()


            # filter by boundary
            if (buffer_lat is not None) and (buffer_lon is not None) and (buffer_distance is not None):
                logging.info("Making spatial extract")
                copy_db_conn.create_function("find_distance", 4, wgs84_distance)
                copy_db_conn.execute('DELETE FROM stops WHERE '
                                     'stop_I NOT IN (select stops.stop_I from stop_times, stops, '
                                     '(select trip_I, min(seq) as min_seq, max(seq) as max_seq from stop_times, stops '
                                     'where stop_times.stop_I = stops.stop_I and CAST(find_distance(lat, lon, ?, ?) AS INT) < ? '
                                     'group by trip_I) q1 '
                                     'where stop_times.stop_I = stops.stop_I and stop_times.trip_I = q1.trip_I and seq >= min_seq and seq <= max_seq '
                                     ')', (buffer_lat, buffer_lon, buffer_distance))

                copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                     'stop_I NOT IN (SELECT stop_I FROM stops)')
                #delete trips with only one stop
                copy_db_conn.execute('DELETE FROM stop_times WHERE '
                                     'trip_I IN (select trip_I from '
                                     '(select trip_I, count(*) as N_stops from stop_times '
                                     'group by trip_I) q1 '
                                     'where N_stops = 1)')

                copy_db_conn.execute('DELETE FROM trips WHERE '
                                     'trip_I NOT IN (SELECT trip_I FROM stop_times)')
                copy_db_conn.execute('DELETE FROM routes WHERE '
                                     'route_I NOT IN (SELECT route_I FROM trips)')
                copy_db_conn.execute('DELETE FROM agencies WHERE '
                                     'agency_I NOT IN (SELECT agency_I FROM routes)')
                copy_db_conn.execute('DELETE FROM shapes WHERE '
                                     'shape_id NOT IN (SELECT shape_id FROM trips)')
                copy_db_conn.execute('DELETE FROM stops_rtree WHERE '
                                     'stop_I NOT IN (SELECT stop_I FROM stops)')
                copy_db_conn.execute('DELETE FROM stop_distances WHERE '
                                     'from_stop_I NOT IN (SELECT stop_I FROM stops)'
                                     'OR to_stop_I NOT IN (SELECT stop_I FROM stops)')
                copy_db_conn.commit()


            # select the largest and smallest seq value for each trip that is within boundary
            # WITH query that includes all stops that are within area or stops of routes that leaves and then returns to area
            # DELETE from stops where not in WITH query
            # Cascade for other tables

            # Update metadata
            if update_metadata:
                logging.info("Updating metadata")
                G_copy = GTFS(tempfile)
                G_copy.meta['copied_from'] = this_db_path
                G_copy.meta['copy_time_ut'] = time.time()
                G_copy.meta['copy_time'] = time.ctime()

                # Copy some keys directly.
                for key in ['original_gtfs', 'download_date', 'location_name',
                            'timezone', ]:
                    G_copy.meta[key] = self.meta[key]
                # Update *all* original metadata under orig_ namespace.
                G_copy.meta.update(('orig_' + k, v) for k, v in self.meta.items())
                G_copy.calc_and_store_stats()

                # print "Vacuuming..."
                copy_db_conn.execute('VACUUM;')
                # print "Analyzing..."
                copy_db_conn.execute('ANALYZE;')
                copy_db_conn.commit()

        return

    def get_cursor(self):
        """
        Return a cursor to the underlying sqlite3 object
        """
        return self.conn.cursor()

    def get_table(self, table_name):
        """
        Return a Pandas dataframe corresponding to a table

        Parameters
        ----------
        table_name: str
            name of the table in the database

        Returns
        -------
        df : pandas.DataFrame
            A pandas dataframe describing the database.
        """
        return pd.read_sql("SELECT * FROM " + table_name, self.conn)

    def get_table_names(self):
        """
        Return a list of the underlying database names

        Returns
        -------
        table_names: a list of the underlying datbase names
        """
        return list(pd.read_sql("SELECT * FROM main.sqlite_master WHERE type='table'", self.conn)["name"])

    def tzset(self):
        """
        This function queries a GTFS connection, finds the timezone of this
        database, and sets it in the TZ environment variable.  This is a
        process-global configuration, by the nature of the C library!

        Returns
        -------
        None

        Alters os.environ['TZ']
        """
        TZ = self.conn.execute('SELECT timezone FROM agencies LIMIT 1').fetchall()[0][0]
        # print TZ
        os.environ['TZ'] = TZ
        time.tzset()  # Cause C-library functions to notice the update.

    def get_timezone_name(self):
        """
        Get name of the GTFS timezone

        Returns
        -------
        timezone_name : str
            name of the time zone, e.g. "Europe/Helsinki"
        """
        tz_name = self.conn.execute('SELECT timezone FROM agencies LIMIT 1'
                                    ).fetchone()
        if tz_name is None:
            raise ValueError("This database does not have a timezone defined.")
        return tz_name[0]

    def get_timezone_string(self, dt=None):
        """
        Return the timezone of the GTFS database object as a string.
        The assumed time when the timezone (difference) is computed
        is the download date of the file.
        This might not be optimal in all cases.

        So this function should return values like:
            "+0200" or "-1100"

        Parameters
        ----------
        dt : datetime.datetime, optional
            The (unlocalized) date when the timezone should be computed.
            Defaults first to download_date, and then to the runtime date.

        Returns
        -------
        tzstring : str
        """
        if dt is None:
            download_date = self.meta.get('download_date')
            if download_date:
                dt = datetime.datetime.strptime(download_date, '%Y-%m-%d')
            else:
                dt = datetime.datetime.today()
        loc_dt = self._timezone.localize(dt)
        # get the timezone
        tzstring = loc_dt.strftime("%z")
        return tzstring

    def ut_seconds_to_gtfs_datetime(self, unixtime):
        """Unixtime to localized datetime

        input:  int (unixtime)
        output: datetime (tz=GTFS timezone)
        """
        return datetime.datetime.fromtimestamp(unixtime, self._timezone)

    def unlocalized_datetime_to_ut_seconds(self, unloc_t):
        """
        Convert datetime (in GTFS timezone) to unixtime

        Parameters
        ----------
        unloc_t : datetime (tz coerced to GTFS timezone, should NOT be UTC.)

        Returns
        -------
        output : int (unixtime)
        """
        loc_dt = self._timezone.localize(unloc_t)
        unixtime_seconds = calendar.timegm(loc_dt.utctimetuple())
        return unixtime_seconds

    def get_day_start_ut(self, date):
        """
        Get day start time (as specified by GTFS) as unix time in seconds

        Parameters
        ----------
        date : str, unicode, or datetime.datetime
            something describing the date

        Returns
        -------
        day_start_ut : int
            start time of the day in unixtime
        """
        if isinstance(date, (str, unicode)):
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
        date_noon = datetime.datetime(date.year, date.month, date.day,
                                      12, 0, 0)
        ut_noon = self.unlocalized_datetime_to_ut_seconds(date_noon)
        return ut_noon - 43200  # 43200=12*60*60 (this comes from GTFS: noon-12 hrs)

    def get_trip_trajectories_within_timespan(self, start, end, use_shapes=True, filter_name=None):
        """
        Get complete trip data for visualizing public transport operation based on gtfs.

        Parameters
        ----------
        start: number
            Earliest position data to return (in unix time)
        end: number
            Latest position data to return (in unix time)
        use_shapes: bool, optional
            Whether or not shapes should be included
        filter_name: str
            Pick only routes having this name.

        Returns
        -------
        trips: dict
            trips['trips'] is a list whose each element (e.g. el = trips['trips'][0])
            is a dict with the following properties:
                el['lats'] -- list of latitudes
                el['lons'] -- list of longitudes
                el['times'] -- list of passage_times
                el['route_type'] -- type of vehicle as specified by GTFS
                el['name'] -- name of the route
        """
        trips = []
        trip_df = self.get_tripIs_active_in_range(start, end)
        print "gtfs_viz.py: fetched " + str(len(trip_df)) + " trip ids"
        shape_cache = {}

        # loop over all trips:
        for row in trip_df.itertuples():
            trip_I = row.trip_I
            day_start_ut = row.day_start_ut
            shape_id = row.shape_id

            trip = {}

            name, route_type = self.get_route_name_and_type_of_tripI(trip_I)
            trip['route_type'] = int(route_type)
            trip['name'] = unicode(name)

            if filter_name and (name != filter_name):
                continue

            stop_lats = []
            stop_lons = []
            stop_dep_times = []
            shape_breaks = []
            stop_seqs = []

            # get stop_data and store it:
            stop_time_df = self.get_trip_stop_time_data(trip_I, day_start_ut)
            for stop_row in stop_time_df.itertuples():
                stop_lats.append(stop_row.lat)
                stop_lons.append(stop_row.lon)
                stop_dep_times.append(stop_row.dep_time_ut)
                stop_seqs.append(stop_row.seq)
                shape_breaks.append(stop_row.shape_break)

            if use_shapes:
                # get shape data (from cache, if possible)
                if shape_id not in shape_cache:
                    shape_cache[shape_id] = shapes.get_shape_points2(self.conn.cursor(), shape_id)
                shape_data = shape_cache[shape_id]
                try:
                    trip['times'] = shapes.interpolate_shape_times(shape_data['d'], shape_breaks, stop_dep_times)
                    trip['lats'] = shape_data['lats']
                    trip['lons'] = shape_data['lons']
                    start_break = shape_breaks[0]
                    end_break = shape_breaks[-1]
                    trip['times'] = trip['times'][start_break:end_break + 1]
                    trip['lats'] = trip['lats'][start_break:end_break + 1]
                    trip['lons'] = trip['lons'][start_break:end_break + 1]
                except Exception as e:
                    # In case interpolation fails:
                    trip['times'] = stop_dep_times
                    trip['lats'] = stop_lats
                    trip['lons'] = stop_lons
            else:
                trip['times'] = stop_dep_times
                trip['lats'] = stop_lats
                trip['lons'] = stop_lons

            trips.append(trip)
        return {"trips": trips}

    def get_stop_count_data(self, start_ut, end_ut):
        """
        Get stop count data.

        Parameters
        ----------
        start_ut : int
            start time in unixtime
        end_ut : int
            end time in unixtime

        Returns
        -------
        stopData : pandas.DataFrame
            each row in the stopData dataFrame is a dictionary with the following elements
                stop_I, count, lat, lon, name
            with data types
                (int, int, float, float, str)
        """
        # TODO! this function could perhaps be made a single sql query now with the new tables?
        trips_df = self.get_tripIs_active_in_range(start_ut, end_ut)
        # stop_I -> count, lat, lon, name
        stop_counts = Counter()

        # loop over all trips:
        for row in trips_df.itertuples():
            # get stop_data and store it:
            stops_seq = self.get_trip_stop_time_data(row.trip_I, row.day_start_ut)
            for stop_time_row in stops_seq.itertuples(index=False):
                if (stop_time_row.dep_time_ut >= start_ut) and (stop_time_row.dep_time_ut <= end_ut):
                    stop_counts[stop_time_row.stop_I] += 1

        all_stop_data = self.get_stop_info()
        counts = [stop_counts[stop_I] for stop_I in all_stop_data["stop_I"].values]

        all_stop_data.loc[:, "count"] = pd.Series(counts, index=all_stop_data.index)
        return all_stop_data

    def get_segment_count_data(self, start, end, use_shapes=True):
        """
        Get segment data including PTN vehicle counts per segment that are
        fully _contained_ within the interval (start, end)

        Parameters
        ----------
        start : int
            start time of the simulation in unix time
        end : int
            end time of the simulation in unix time
        use_shapes : bool, optional
            whether to include shapes (if available)

        Returns
        -------
        seg_data : list
            each element in the list is a dict containing keys:
                "trip_I", "lats", "lons", "shape_id", "stop_seqs", "shape_breaks"
        """
        cur = self.conn.cursor()
        # get all possible trip_ids that take place between start and end
        trips_df = self.get_tripIs_active_in_range(start, end)
        # stop_I -> count, lat, lon, name
        segment_counts = Counter()
        seg_to_info = {}
        # tripI_to_seq = "inverted segToShapeData"
        tripI_to_seq = defaultdict(list)

        # loop over all trips:
        for row in trips_df.itertuples():
            # get stop_data and store it:
            stops_df = self.get_trip_stop_time_data(row.trip_I, row.day_start_ut)
            for i in range(len(stops_df) - 1):
                (stop_I, dep_time_ut, s_lat, s_lon, s_seq, shape_break) = stops_df.iloc[i]
                (stop_I_n, dep_time_ut_n, s_lat_n, s_lon_n, s_seq_n, shape_break_n) = stops_df.iloc[i + 1]
                # test if _contained_ in the interval
                # overlap would read:
                #   (dep_time_ut <= end) and (start <= dep_time_ut_n)
                if (dep_time_ut >= start) and (dep_time_ut_n <= end):
                    seg = (stop_I, stop_I_n)
                    segment_counts[seg] += 1
                    if seg not in seg_to_info:
                        seg_to_info[seg] = {
                            "trip_I": row.trip_I,
                            "lats": [s_lat, s_lat_n],
                            "lons": [s_lon, s_lon_n],
                            "shape_id": row.shape_id,
                            "stop_seqs": [s_seq, s_seq_n],
                            "shape_breaks": [shape_break, shape_break_n]
                        }
                        tripI_to_seq[row.trip_I].append(seg)
        print len(segment_counts)

        stop_names = {}
        for (stop_I, stop_J) in segment_counts.keys():
            for s in [stop_I, stop_J]:
                if s not in stop_names:
                    pdframe = self.get_stop_info(s)
                    stop_names[s] = pdframe['name'].values[0]

        seg_data = []
        for seg, count in segment_counts.items():
            segInfo = seg_to_info[seg]
            shape_breaks = segInfo["shape_breaks"]
            seg_el = {}
            if use_shapes and shape_breaks and shape_breaks[0] and shape_breaks[1]:
                shape = shapes.get_shape_between_stops(
                    cur,
                    segInfo['trip_I'],
                    shape_breaks=shape_breaks
                )
                seg_el['lats'] = segInfo['lats'][:1] + shape['lat'] + segInfo['lats'][1:]
                seg_el['lons'] = segInfo['lons'][:1] + shape['lon'] + segInfo['lons'][1:]
            else:
                seg_el['lats'] = segInfo['lats']
                seg_el['lons'] = segInfo['lons']
            seg_el['name'] = stop_names[seg[0]] + "-" + stop_names[seg[1]]
            seg_el['count'] = count
            seg_data.append(seg_el)
        return seg_data

    def get_all_route_shapes(self, use_shapes=True):
        """
        Get the shapes of all routes.

        Parameters
        ----------
        use_shapes : bool, optional
            by default True (i.e. use shapes as the name of the function indicates)
            if False (fall back to lats and longitudes)

        Returns
        -------
        routeShapes: list of dicts that should have the following keys
            name, type, agency, lats, lons
            with types
            list, list, str, list, list
        """
        cur = self.conn.cursor()
        # all shape_id:s corresponding to a route_I:
        # query = "SELECT DISTINCT name, shape_id, trips.route_I, route_type
        #          FROM trips LEFT JOIN routes USING(route_I)"
        # data1 = pd.read_sql_query(query, self.conn)
        # one (arbitrary) shape_id per route_I ("one direction") -> less than half of the routes
        query = "SELECT routes.name as name, shape_id, route_I, trip_I, routes.type, " \
                "        agency_id, agencies.name as agency_name " \
                "FROM trips " \
                "LEFT JOIN routes " \
                "USING(route_I) " \
                "LEFT JOIN agencies " \
                "USING(agency_I) " \
                "GROUP BY routes.route_I"
        data = pd.read_sql_query(query, self.conn)
        routeShapes = []
        n_rows = len(data)
        for i, row in enumerate(data.itertuples()):
            datum = {"name": row.name, "type": row.type, "agency": row.agency_id, "agency_name": row.agency_name}
            print row.agency_id, ": ",  i, "/", n_rows
            # this function should be made also non-shape friendly (at this point)
            if use_shapes and row.shape_id:
                shape = shapes.get_shape_points2(cur, row.shape_id)
                lats = shape['lats']
                lons = shape['lons']
            else:
                stop_shape = self.get_trip_stop_latlons(row.trip_I)
                lats = list(stop_shape['lat'])
                lons = list(stop_shape['lon'])
            datum['lats'] = lats
            datum['lons'] = lons
            routeShapes.append(datum)
        return routeShapes

    def get_tripIs_active_in_range(self, start, end):
        """
        Obtain from the (standard) GTFS database, list of trip_IDs (and other trip_related info)
        that are active between given 'start' and 'end' times.

        The start time of a trip is determined by the departure time at the last stop of the trip.
        The end time of a trip is determined by the arrival time at the last stop of the trip.

        Parameters
        ----------
        start, end : int
            the start and end of the time interval in unix time seconds

        Returns
        -------
        active_trips : pandas.DataFrame with columns
            trip_I, day_start_ut, start_time_ut, end_time_ut, shape_id
        """
        to_select = "trip_I, day_start_ut, start_time_ut, end_time_ut, shape_id "
        query = "SELECT " + to_select + \
                     "FROM day_trips " \
                     "WHERE " \
                     "(end_time_ut > {start_ut} AND start_time_ut < {end_ut})".format(start_ut=start, end_ut=end)
        return pd.read_sql_query(query, self.conn)

    def get_trip_counts_per_day(self):
        """
        Get trip counts per day between the start and end day of hte feed.

        Returns
        -------
        trip_counts : pandas.DataFrame
            has columns "dates" and "trip_counts" where
                dates are strings
                trip_counts are ints
        """
        query = "SELECT date, count(*) AS number_of_trips FROM day_trips GROUP BY date"
        # this yields the actual data
        trip_counts_per_day = pd.read_sql_query(query, self.conn, index_col="date")
        # the rest is simply code for filling out "gaps" in the timeline
        # (necessary for visualizations sometimes)
        max_day = trip_counts_per_day.index.max()
        min_day = trip_counts_per_day.index.min()
        min_date = datetime.datetime.strptime(min_day, '%Y-%m-%d')
        max_date = datetime.datetime.strptime(max_day, '%Y-%m-%d')
        num_days = (max_date - min_date).days
        dates = [min_date + datetime.timedelta(days=x) for x in range(num_days + 1)]
        trip_counts = []
        date_strs = []
        for date in dates:
            datestr = date.strftime("%Y-%m-%d")
            date_strs.append(datestr)
            try:
                value = trip_counts_per_day.loc[datestr, 'number_of_trips']
            except KeyError:
                # set value to 0 if dsut is not present, i.e. when no trips
                # take place on that day
                value = 0
            trip_counts.append(value)
        for datestr in trip_counts_per_day.index:
            assert datestr in date_strs
        data = {"dates": date_strs, "trip_counts": trip_counts}
        return pd.DataFrame(data)

        # Remove these pieces of code when this function has been tested:
        #
        # (RK) not sure if this works or not:
        # def localized_datetime_to_ut_seconds(self, loc_dt):
        #     utcoffset = loc_dt.utcoffset()
        #     print utcoffset
        #     utc_naive  = loc_dt.replace(tzinfo=None) - utcoffset
        #     timestamp = (utc_naive - datetime.datetime(1970, 1, 1)).total_seconds()
        #     return timestamp

        # def
        # query = "SELECT day_start_ut, count(*) AS number_of_trips FROM day_trips GROUP BY day_start_ut"
        # trip_counts_per_day = pd.read_sql_query(query, self.conn, index_col="day_start_ut")
        # min_day_start_ut = trip_counts_per_day.index.min()
        # max_day_start_ut = trip_counts_per_day.index.max()
        # spacing = 24*3600
        # # day_start_ut is noon - 12 hours (to cover for daylight saving time changes)
        # min_date_noon = self.ut_seconds_to_gtfs_datetime(min_day_start_ut)+datetime.timedelta(hours=12)
        # max_date_noon = self.ut_seconds_to_gtfs_datetime(max_day_start_ut)+datetime.timedelta(hours=12)
        # num_days = (max_date_noon-min_date_noon).days
        # print min_date_noon, max_date_noon
        # dates_noon = [min_date_noon + datetime.timedelta(days=x) for x in range(0, num_days+1)]
        # day_noon_uts = [int(self.localized_datetime_to_ut_seconds(date)) for date in dates_noon]
        # day_start_uts = [dnu-12*3600 for dnu in day_noon_uts]
        # print day_start_uts
        # print list(trip_counts_per_day.index.values)

        # assert max_day_start_ut == day_start_uts[-1]
        # assert min_day_start_ut == day_start_uts[0]

        # trip_counts = []
        # for dsut in day_start_uts:
        #     try:
        #         value = trip_counts_per_day.loc[dsut, 'number_of_trips']
        #     except KeyError as e:
        #         # set value to 0 if dsut is not present, i.e. when no trips
        #         # take place on that day
        #         value = 0
        #     trip_counts.append(value)
        # for dsut in trip_counts_per_day.index:
        #     assert dsut in day_start_uts
        # return {"day_start_uts": day_start_uts, "trip_counts":trip_counts}

    def get_spreading_trips(self, start_time_ut, lat, lon,
                            max_duration_ut=4 * 3600,
                            min_transfer_time=30,
                            shapes=False):
        """
        Starting from a specific point and time, get complete single source
        shortest path spreading dynamics as trips, or "events".

        Parameters
        ----------
        start_time_ut: number
            Start time of the spreading.
        lat: float
            latitude of the spreading seed location
        lon: float
            longitude of the spreading seed location
        max_duration_ut: int
            maximum duration of the spreading process (in seconds)
        min_transfer_time : int
            minimum transfer time in seconds
        shapes : bool
            whether to include shapes

        Returns
        -------
        trips: dict
            trips['trips'] is a list whose each element (e.g. el = trips['trips'][0])
            is a dict with the following properties:
                el['lats'] : list of latitudes
                el['lons'] : list of longitudes
                el['times'] : list of passage_times
                el['route_type'] : type of vehicle as specified by GTFS, or -1 if walking
                el['name'] : name of the route
        """
        # events are sorted by arrival time, so in order to use the
        # heapq, we need to have events coded as
        # (arrival_time, (from_stop, to_stop))
        start_stop_I = self.get_closest_stop(lat, lon)
        end_time_ut = start_time_ut + max_duration_ut

        print "Computing/fetching events"
        events_df = self.get_transit_events(start_time_ut, end_time_ut)
        all_stops = set(self.get_stop_info()['stop_I'])

        uninfected_stops = all_stops.copy()
        uninfected_stops.remove(start_stop_I)

        # match stop_I to a more advanced stop object
        seed_stop = SpreadingStop(start_stop_I, min_transfer_time)

        stop_I_to_spreading_stop = {
            start_stop_I: seed_stop
        }
        for stop in uninfected_stops:
            stop_I_to_spreading_stop[stop] = SpreadingStop(stop, min_transfer_time)

        # get for each stop their
        walk_speed = 0.5  # meters/second

        print "intializing heap"
        event_heap = Heap(events_df)

        start_event = Event(start_time_ut - 1,
                            start_time_ut - 1,
                            start_stop_I,
                            start_stop_I,
                            -1)

        seed_stop.visit(start_event)
        assert len(seed_stop.visit_events) > 0
        event_heap.add_event(start_event)
        event_heap.add_walk_events_to_heap(self, start_event,
                                           start_time_ut, walk_speed,
                                           uninfected_stops, max_duration_ut)

        i = 1

        while event_heap.size() > 0 and len(uninfected_stops) > 0:
            e = event_heap.pop_next_event()
            this_stop = stop_I_to_spreading_stop[e.from_stop_I]

            if e.arr_time_ut > start_time_ut + max_duration_ut:
                break

            if this_stop.can_infect(e):

                target_stop = stop_I_to_spreading_stop[e.to_stop_I]
                already_visited = target_stop.has_been_visited()
                target_stop.visit(e)

                if not already_visited:
                    uninfected_stops.remove(e.to_stop_I)
                    print i, event_heap.size()
                    event_heap.add_walk_events_to_heap(self, e, start_time_ut,
                                                       walk_speed, uninfected_stops,
                                                       max_duration_ut)
                    i += 1

        # create new transfer events and add them to the heap (=queue)
        inf_times = [[stop_I, el.get_min_visit_time() - start_time_ut]
                     for stop_I, el in stop_I_to_spreading_stop.items()]
        inf_times = numpy.array(inf_times)
        inf_time_data = pd.DataFrame(inf_times, columns=["stop_I", "inf_time_ut"])
        stop_data = self.get_stop_info()

        combined = inf_time_data.merge(stop_data, how='inner', on='stop_I', suffixes=('_infs', '_stops'), copy=True)

        trips = []
        for stop_I, dest_stop_obj in stop_I_to_spreading_stop.iteritems():
            inf_event = dest_stop_obj.get_min_event()
            if inf_event is None:
                continue
            dep_stop_I = inf_event.from_stop_I
            dep_lat = float(combined[combined['stop_I'] == dep_stop_I]['lat'].values)
            dep_lon = float(combined[combined['stop_I'] == dep_stop_I]['lon'].values)

            dest_lat = float(combined[combined['stop_I'] == stop_I]['lat'].values)
            dest_lon = float(combined[combined['stop_I'] == stop_I]['lon'].values)

            if inf_event.trip_I == -1:
                name = "walk"
                rtype = -1
            else:
                name, rtype = self.get_route_name_and_type_of_tripI(inf_event.trip_I)

            trip = {
                "lats": [dep_lat, dest_lat],
                "lons": [dep_lon, dest_lon],
                "times": [inf_event.dep_time_ut, inf_event.arr_time_ut],
                "name": name,
                "route_type": rtype
            }
            trips.append(trip)
        return {"trips": trips}

    def get_closest_stop(self, lat, lon):
        """
        Get closest stop to a given location.

        Parameters
        ----------
        lat: float
            latitude coordinate of the location
        lon: float
            longitude coordinate of the location

        Returns
        -------
        stop_I: int
            the index of the stop in the database
        """
        cur = self.conn.cursor()
        min_dist = float("inf")
        min_stop_I = None
        rows = cur.execute("SELECT stop_I, lat, lon FROM stops")
        for stop_I, lat_s, lon_s in rows:
            dist_now = wgs84_distance(lat, lon, lat_s, lon_s)
            if dist_now < min_dist:
                min_dist = dist_now
                min_stop_I = stop_I
        return min_stop_I

    def get_route_name_and_type_of_tripI(self, trip_I):
        """
        Get route short name and type

        Parameters
        ----------
        trip_I: int
            short trip index created when creating the database

        Returns
        -------
        name: str
            short name of the route, eg. 195N
        type: int
            route_type according to the GTFS standard
        """
        cur = self.conn.cursor()
        results = cur.execute("SELECT name, type FROM routes JOIN trips USING(route_I) WHERE trip_I=(?)", (trip_I,))
        name, rtype = results.fetchone()
        return unicode(name), int(rtype)

    def get_route_name_and_type(self, route_I):
        """
        Get route short name and type

        Parameters
        ----------
        route_I: int
            route index (database specific)

        Returns
        -------
        name: str
            short name of the route, eg. 195N
        type: int
            route_type according to the GTFS standard
        """
        cur = self.conn.cursor()
        results = cur.execute("SELECT name, type FROM routes WHERE route_I=(?)", (route_I,))
        name, rtype = results.fetchone()
        return unicode(name), int(rtype)


    def get_trip_stop_latlons(self, trip_I):
        """
        Get coordinates for a given trip_I

        Parameters
        ----------
        trip_I : int
            the integer id of the trip

        Returns
        -------
        stop_coords : pandas.DataFrame
            with columns "lats" and "lons"
        """
        query = """SELECT lat, lon
                    FROM stop_times
                    JOIN stops
                    USING(stop_I)
                        WHERE trip_I={trip_I}
                    ORDER BY stop_times.seq""".format(trip_I=trip_I)
        stop_coords = pd.read_sql(query, self.conn)
        return stop_coords

    def get_trip_stop_time_data(self, trip_I, day_start_ut):
        """
        Obtain from the (standard) GTFS database, trip stop data
        (departure time in ut, lat, lon, seq, shape_break) as a pandas DataFrame

        Some filtering could be applied here, if only e.g. departure times
        corresponding within some time interval should be considered.

        Parameters
        ----------
        trip_I : int
            integer index of the trip
        day_start_ut : int
            the start time of the day in unix time (seconds)

        Returns
        -------
        df: pandas.DataFrame
            df has the following columns
            'departure_time_ut, lat, lon, seq, shape_break'
        """
        to_select = "stop_I, " + str(day_start_ut) + "+dep_time_ds AS dep_time_ut, lat, lon, seq, shape_break"
        str_to_run = "SELECT " + to_select + """
                        FROM stop_times JOIN stops USING(stop_I)
                        WHERE (trip_I ={trip_I}) ORDER BY seq
                      """
        str_to_run = str_to_run.format(trip_I=trip_I)
        return pd.read_sql_query(str_to_run, self.conn)

    def get_events_by_tripI_and_dsut(self, trip_I, day_start_ut,
                                     start_ut=None, end_ut=None):
        """
        Get trip data as a list of events (i.e. dicts).

        Parameters
        ----------
        trip_I : int
            shorthand index of the trip.
        day_start_ut : int
            the start time of the day in unix time (seconds)
        start_ut : int, optional
            consider only events that start after this time
            If not specified, this filtering is not applied.
        end_ut : int, optional
            Consider only events that end before this time
            If not specified, this filtering is not applied.

        Returns
        -------
        events: list of dicts
            each element contains the following data:
                from_stop: int (stop_I)
                to_stop: int (stop_I)
                dep_time_ut: int (in unix time)
                arr_time_ut: int (in unix time)
        """
        # for checking input:
        assert day_start_ut <= start_ut
        assert day_start_ut <= end_ut
        assert start_ut <= end_ut
        events = []
        # check that trip takes place on that day:
        if not self.tripI_takes_place_on_dsut(trip_I, day_start_ut):
            return events

        query = """SELECT stop_I, arr_time_ds+?, dep_time_ds+?
                    FROM stop_times JOIN stops USING(stop_I)
                    WHERE
                        (trip_I = ?)
                """
        params = [day_start_ut, day_start_ut,
                  trip_I]
        if start_ut:
            query += "AND (dep_time_ds > ?-?)"
            params += [start_ut, day_start_ut]
        if end_ut:
            query += "AND (arr_time_ds < ?-?)"
            params += [end_ut, day_start_ut]
        query += "ORDER BY arr_time_ds"
        cur = self.conn.cursor()
        rows = cur.execute(query, params)
        stop_data = list(rows)
        for i in range(len(stop_data) - 1):
            event = {
                "from_stop": stop_data[i][0],
                "to_stop": stop_data[i + 1][0],
                "dep_time_ut": stop_data[i][2],
                "arr_time_ut": stop_data[i + 1][1]
            }
            events.append(event)
        return events

    def tripI_takes_place_on_dsut(self, trip_I, day_start_ut):
        """
        Check that a trip takes place during a day

        Parameters
        ----------
        trip_I : int
            index of the trip in the gtfs data base
        day_start_ut : int
            the starting time of the day in unix time (seconds)

        Returns
        -------
        takes_place: bool
            boolean value describing whether the trip takes place during
            the given day or not
        """
        query = "SELECT * FROM days WHERE trip_I=? AND day_start_ut=?"
        params = (trip_I, day_start_ut)
        cur = self.conn.cursor()
        rows = list(cur.execute(query, params))
        if len(rows) == 0:
            return False
        else:
            assert len(rows) == 1, 'On a day, a trip_I should be present at most once'
            return True

    # unused and (untested) code:
    #
    # def get_tripIs_from_stopI_within_time_range(self, stop_I, day_start_ut, start_ut, end_ut):
    #     """
    #     Obtain a list of trip_Is that go through some stop during a given time.
    #
    #     Parameters
    #     ----------
    #     stop_I : int
    #         index of the stop to be considered
    #     day_start_ut : int
    #         start of the day in unix time (seconds)
    #     start_ut: int
    #         the first possible departure time from the stop
    #         in unix time (seconds)
    #     end_ut: int
    #         the last possible departure time from the stop
    #         in unix time (seconds)
    #
    #     Returns
    #     -------
    #     trip_Is: list
    #         list of integers (trip_Is)
    #     """
    #     start_ds = start_ut - day_start_ut
    #     end_ds = end_ut - day_start_ut
    #     # is the _distinct_ relly required?
    #     query = "SELECT distinct(trip_I) " \
    #             "FROM days " \
    #             "JOIN stop_times " \
    #             "USING(trip_I) " \
    #             "WHERE (days.day_start_ut == ?)" \
    #             "AND (stop_times.stop_I=?) " \
    #             "AND (stop_times.dep_time_ds >= ?) " \
    #             "AND (stop_times.dep_time_ds <= ?)"
    #     params = (day_start_ut, stop_I, start_ds, end_ds)
    #     cur = self.conn.cursor()
    #     trip_Is = [el[0] for el in cur.execute(query, params)]
    #     return trip_Is

    def day_start_ut(self, ut):
        """
        Convert unixtime to unixtime on GTFS start-of-day.

        GTFS defines the start of a day as "noon minus 12 hours" to solve
        most DST-related problems. This means that on DST-changing days,
        the day start isn't midnight. This function isn't idempotent.
        Running it twice on the "move clocks backwards" day will result in
        being one day too early.

        Parameters
        ----------
        ut: int
            Unixtime

        Returns
        -------
        ut: int
            Unixtime corresponding to start of day
        """
        # set timezone to the one of gtfs
        self.tzset()
        # last -1 equals to 'not known' for DST (automatically deduced then)
        return time.mktime(time.localtime(ut)[:3] + (12, 00, 0, 0, 0, -1)) - 43200

    def increment_daystart_ut_by_ndays(self, daystart_ut, n_days=1):
        """Increment the GTFS-definition of "day start".

        Parameters
        ----------
        daystart_ut : int
            unixtime of the previous start of day.  If this time is between
            12:00 or greater, there *will* be bugs.  To solve this, run the
            input through day_start_ut first.
        n_days: int
            number of days to increment
        """
        self.tzset()
        day0 = time.localtime(daystart_ut + 43200)  # time of noon
        dayN = time.mktime(day0[:2] +  # YYYY, MM
                           (day0[2] + n_days,) +  # DD
                           (12, 00, 0, 0, 0, -1)) - 43200  # HHMM, etc.  Minus 12 hours.
        return dayN

    def _get_possible_day_starts(self, start_ut, end_ut, max_time_overnight=None):
        """
        Get all possible day start times between start_ut and end_ut
        Currently this function is used only by get_tripIs_within_range_by_dsut

        Parameters
        ----------
        start_ut : int
            start time in unix time
        end_ut : int
            end time in unix time
        max_time_overnight : int
            the maximum length of time that a trip can take place on
            during the next day (i.e. after midnight run times like 25:35)

        Returns
        -------
        day_start_times_ut : list
            list of ints (unix times in seconds) for returning all possible day
            start times
        start_times_ds : list
            list of ints (unix times in seconds) stating the valid start time in
            day seconds
        end_times_ds : list
            list of ints (unix times in seconds) stating the valid end times in
            day_seconds
        """
        if max_time_overnight is None:
            # 7 hours:
            max_time_overnight = 7 * 60 * 60

        # sanity checks for the timezone parameter
        # assert timezone < 14
        # assert timezone > -14
        # tz_seconds = int(timezone*3600)
        assert start_ut < end_ut
        start_day_ut = self.day_start_ut(start_ut)
        # start_day_ds = int(start_ut+tz_seconds) % seconds_in_a_day  #??? needed?
        start_day_ds = start_ut - start_day_ut
        # assert (start_day_ut+tz_seconds) % seconds_in_a_day == 0
        end_day_ut = self.day_start_ut(end_ut)
        # end_day_ds = int(end_ut+tz_seconds) % seconds_in_a_day    #??? needed?
        # end_day_ds = end_ut - end_day_ut
        # assert (end_day_ut+tz_seconds) % seconds_in_a_day == 0

        # If we are early enough in a day that we might have trips from
        # the previous day still running, decrement the start day.
        if start_day_ds < max_time_overnight:
            start_day_ut = self.increment_daystart_ut_by_ndays(start_day_ut, n_days=-1)

        # day_start_times_ut = range(start_day_ut, end_day_ut+seconds_in_a_day, seconds_in_a_day)

        # Create a list of all possible day start times.  This is roughly
        # range(day_start_ut, day_end_ut+1day, 1day).
        day_start_times_ut = [start_day_ut]
        while day_start_times_ut[-1] < end_day_ut:
            day_start_times_ut.append(self.increment_daystart_ut_by_ndays(day_start_times_ut[-1]))

        start_times_ds = []
        end_times_ds = []
        # For every possible day start:
        for dstu in day_start_times_ut:
            # start day_seconds starts at either zero, or time-daystart
            daystart_ut = max(0, start_ut - dstu)
            start_times_ds.append(daystart_ut)
            # end day_seconds is time-day_start
            day_end_ut = end_ut - dstu
            end_times_ds.append(day_end_ut)
        # Return three tuples which can be zip:ped together.
        return day_start_times_ut, start_times_ds, end_times_ds

    def get_tripIs_within_range_by_dsut(self,
                                        start_time_ut,
                                        end_time_ut):
        """
        Obtain a list of trip_Is that take place during a time interval.
        The trip needs to be only partially overlapping with the given time interval.
        The grouping by dsut (day_start_ut) is required as same trip_I could
        take place on multiple days.

        Parameters
        ----------
        start_time_ut : int
            start of the time interval in unix time (seconds)
        end_time_ut: int
            end of the time interval in unix time (seconds)

        Returns
        -------
        trip_I_dict: dict
            keys: day_start_times to list of integers (trip_Is)
        """
        cur = self.conn.cursor()
        assert start_time_ut <= end_time_ut
        dst_ut, st_ds, et_ds = \
            self._get_possible_day_starts(start_time_ut, end_time_ut, 7)
        assert len(dst_ut) >= 0
        trip_I_dict = {}
        for day_start_ut, start_ds, end_ds in \
                zip(dst_ut, st_ds, et_ds):
            query = """
                        SELECT distinct(trip_I)
                        FROM days
                            JOIN trips
                            USING(trip_I)
                        WHERE
                            (days.day_start_ut == ?)
                            AND (
                                    (trips.start_time_ds <= ?)
                                    AND
                                    (trips.end_time_ds >= ?)
                                )
                        """
            params = (day_start_ut, end_ds, start_ds)
            trip_Is = [el[0] for el in cur.execute(query, params)]
            if len(trip_Is) > 0:
                trip_I_dict[day_start_ut] = trip_Is
        return trip_I_dict

    def get_stop_info(self, stop_I=None):
        """
        Get all stop data as a pandas DataFrame for all stops, or an individual stop'

        Parameters
        ----------
        stop_I : int
            stop index

        Returns
        -------
        df: pandas.DataFrame
        """
        if stop_I is None:
            return pd.read_sql_query("SELECT * FROM stops", self.conn)
        else:
            return pd.read_sql_query("SELECT * FROM stops WHERE stop_I=?",
                                     self.conn, params=(stop_I,))

    def get_transit_events(self, start_time_ut=None, end_time_ut=None):
        """
        Obtain a list of events that take place during a time interval.
        Each event needs to be only partially overlap the given time interval.
        Does not include walking events.

        Parameters
        ----------
        start_time_ut : int
            start of the time interval in unix time (seconds)
        end_time_ut: int
            end of the time interval in unix time (seconds)

        Returns
        -------
        events: pandas.DataFrame
            with the following columns and types
                dep_time_ut: int
                arr_time_ut: int
                from_stop_I: int
                to_stop_I: int
                trip_I : int
                shape_id : int

        See also
        --------
        get_transit_events_in_time_span : an older version of the same thing
        """
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='day_trips2'")
        if len(cur.fetchall()) > 0:
            table_name = "day_trips2"
        else:
            table_name = "day_trips"
        event_query = "SELECT stop_I, seq, trip_I, route_I, routes.route_id AS route_id, routes.type AS route_type, " \
                      "shape_id, day_start_ut+dep_time_ds AS dep_time_ut, day_start_ut+arr_time_ds AS arr_time_ut " \
                      "FROM " + table_name + " " \
                      "JOIN trips USING(trip_I) " \
                      "JOIN routes USING(route_I) " \
                      "JOIN stop_times USING(trip_I)"

        if end_time_ut or start_time_ut:
            event_query += " WHERE "
        prev_where = False
        if end_time_ut:
            event_query += table_name + ".start_time_ut<{end_time_ut}".format(end_time_ut=end_time_ut)
            prev_where = True
        if start_time_ut:
            if prev_where:
                event_query += " AND "
            event_query += "day_trips2.end_time_ut>{start_time_ut}".format(start_time_ut=start_time_ut)
        if end_time_ut:
            if prev_where:
                event_query += " AND "
            prev_where = True
            event_query += "dep_time_ut<={end_time_ut}".format(end_time_ut=end_time_ut)
        if start_time_ut:
            if prev_where:
                event_query += " AND "
            event_query += "arr_time_ut>={start_time_ut}".format(start_time_ut=start_time_ut)
        # ensure ordering
        event_query += " ORDER BY trip_I, day_start_ut+dep_time_ds;"
        # print event_query

        events_result = pd.read_sql_query(event_query, self.conn)
        # 'filter' results so that only real "events" are taken into account

        from_indices = numpy.nonzero(
            ( (events_result['trip_I'][:-1].values == events_result['trip_I'][1:].values))
            * (events_result['seq'][:-1].values < events_result['seq'][1:].values)
        )[0]
        to_indices = from_indices + 1
        # these should have same trip_ids
        assert (events_result['trip_I'][from_indices] == events_result['trip_I'][to_indices]).all()
        trip_Is = events_result['trip_I'][from_indices]
        from_stops = events_result['stop_I'][from_indices]
        to_stops = events_result['stop_I'][to_indices]
        shape_ids = events_result['shape_id'][from_indices]
        dep_times = events_result['dep_time_ut'][from_indices]
        arr_times = events_result['arr_time_ut'][to_indices]
        route_types = events_result['route_type'][from_indices]
        route_ids = events_result['route_id'][from_indices]
        durations = arr_times.values - dep_times.values
        assert (durations >= 0).all()
        from_seqs = events_result['seq'][from_indices]
        to_seqs = events_result['seq'][to_indices]
        data_tuples = zip(from_stops, to_stops, dep_times, arr_times,
                          shape_ids, route_types, route_ids, trip_Is,
                          durations, from_seqs, to_seqs)
        columns = ["from_stop_I", "to_stop_I", "dep_time_ut", "arr_time_ut",
                   "shape_id", "route_type", "route_id", "trip_I",
                   "duration", "from_seq", "to_seq"]
        df = pd.DataFrame.from_records(data_tuples, columns=columns)
        return df

    def get_transit_events_in_time_span(self, start_time_ut, end_time_ut,
                                        fully_contained=False):
        """
        Obtain a list of events that take place during a time interval.
        Each event needs to be only partially overlap the given time interval.
        Does not include walking events.
        NOTE: This is currently extremely slow!

        Parameters
        ----------
        start_time_ut : int
            start of the time interval in unix time (seconds)
        end_time_ut: int
            end of the time interval in unix time (seconds)
        fully_contained: bool, optional (default=False)
           Whether events need to be fully contained in the time interval.

        Returns
        -------
        events: pandas DataFrame
            with the following columns and types
                dep_time_ut: int
                arr_time_ut: int
                from_stop_I: int
                to_stop_I: int
                trip_I : int
        """
        warnings.warn("get_transit_event_in_time_span will probably be deprecated soon", DeprecationWarning)
        # get relevant trips and day_start_times
        day_start_to_trips = \
            self.get_tripIs_within_range_by_dsut(start_time_ut, end_time_ut)
        res_df = pd.DataFrame(columns=['from_stop_I', 'to_stop_I',
                                       'dep_time_ut', 'arr_time_ut',
                                       'trip_I']
                              )
        for day_start_ut, trip_Is in day_start_to_trips.items():
            for trip_I in trip_Is:
                # Manual join is much faster than using the view.
                query = "SELECT " \
                        "stop_I, " \
                        "day_start_ut+dep_time_ds AS dep_time_ut, " \
                        "day_start_ut+arr_time_ds AS arr_time_ut, " \
                        "trip_I " \
                        "FROM days " \
                        "JOIN stop_times " \
                        "USING (trip_I) " \
                        "WHERE trip_I=? " \
                        "AND day_start_ut=? " \
                        "ORDER BY seq"
                params = (trip_I, day_start_ut)
                stop_data_df = pd.read_sql_query(query, self.conn, params=params)
                # check that all events take place within the given timeframe
                # (at least partially)
                valids = (
                    (stop_data_df['arr_time_ut'].iloc[1:].values >= start_time_ut) *
                    (stop_data_df['dep_time_ut'].iloc[:-1].values <= end_time_ut)
                )
                from_stops = stop_data_df['stop_I'].iloc[:-1][valids]
                to_stops = stop_data_df['stop_I'].iloc[1:][valids]
                dep_times_ut = stop_data_df['dep_time_ut'].iloc[:-1][valids]
                arr_times_ut = stop_data_df['arr_time_ut'].iloc[1:][valids]
                trip_Is = stop_data_df['trip_I'].iloc[1:][valids]
                df_dict = {
                    'from_stop_I': from_stops.values,
                    'to_stop_I': to_stops.values,
                    'dep_time_ut': dep_times_ut.values,
                    'arr_time_ut': arr_times_ut.values,
                    'trip_I': trip_Is.values,
                }
                trip_df = pd.DataFrame.from_records(df_dict)
                if res_df is None:
                    res_df = trip_df.copy()
                else:
                    res_df = res_df.append(trip_df, ignore_index=True)
        return res_df

    def get_straight_line_transfer_distances(self, stop_I=None):
        """
        Get (straight line) distances to stations that can be transfered to.

        Parameters
        ----------
        stop_I : int, optional
            If not specified return all possible transfer distances

        Returns
        -------
        distances: pandas.DataFrame
            each row has the following items
                from_stop_I: int
                to_stop_I: int
                d: float or int #distance in meters
        """
        if stop_I is not None:
            query = """ SELECT from_stop_I, to_stop_I, d
                        FROM stop_distances
                            WHERE
                                from_stop_I=?
                    """
            params = (stop_I,)
        else:
            query = """ SELECT from_stop_I, to_stop_I, d
                        FROM stop_distances
                    """
            params = None
        stop_data_df = pd.read_sql_query(query, self.conn, params=params)
        return stop_data_df

    def calc_and_store_stats(self):
        """
        Computes stats and stores them into the underlying sqlite database.

        Returns
        -------
        stats : dict
        """
        stats = self.get_stats()
        self.meta.update(stats)
        self.meta['stats_calc_at_ut'] = time.time()
        return stats

    def print_stats(self):
        """
        Print out basic statistics about a GTFS database

        TODO: Merge with get_stats?
        """
        cur = self.conn.cursor()
        conn = self.conn

        def print_(x, n=None):
            if n:
                print "  " + x.ljust(25) + " = %s" % (n,)
                return
            print "  " + x

        # Basic table counts
        for table in ['agencies', 'routes', 'stops', 'stop_times', 'trips', 'calendar', 'shapes', 'calendar_days', 'days',
                      'stop_distances']:
            n = conn.execute('select count(*) from %s' % table).fetchone()[0]
            print_(("Number of %s" % table).ljust(25) + " = %s" % n, )
        # Stop lat/lon range
        lats = [x[0] for x in conn.execute('select lat from stops').fetchall()]
        lons = [x[0] for x in conn.execute('select lon from stops').fetchall()]
        min_, min10, median, max90, max_ = numpy.percentile(lats, [0, 10, 50, 90, 100])
        minO_, min10O, medianO, max90O, maxO_ = numpy.percentile(lons, [0, 10, 50, 90, 100])

        print_("")
        print_("Stop related")
        print_("Stop lat min/max", (min_, max_))
        print_("Stop lat middle 80%", (min10, max90))
        print_("Height (km)", wgs84_distance(min_, medianO, max_, medianO) / 1000.)
        print_("Height, middle 80% (km)", wgs84_distance(min10, medianO, max90, medianO) / 1000.)
        print_("Stop lon min/max", (minO_, maxO_))
        print_("Stop lon middle 80%", (min10O, max90O))
        print_("Width (km)", wgs84_distance(median, minO_, median, maxO_) / 1000.)
        print_("Width, middle 80% (km)", wgs84_distance(median, min10O, median, max90O) / 1000.)

        print_("")
        print_("Calendar related")
        print_("Start date", cur.execute("select min(start_date) from calendar").fetchone()[0])
        print_("End date", cur.execute("select max(end_date) from calendar").fetchone()[0])
        # print_("Start date", cur.execute("select min(start_date) from calendar"))
        # print_("End date", cur.execute("select max(end_date) from calendar"))

    def write_stats_as_csv(self, path_to_csv):
        """
        Writes data from get_stats to csv file

        Parameters
        ----------
        path_to_csv: filepath to csv file
        """
        import csv
        stats_dict = self.get_stats()
        # check if file exist
        """if not os.path.isfile(path_to_csv):
            is_new = True
        else:
            is_new = False"""
        try:
            with open(path_to_csv, 'rb') as csvfile:
                if list(csv.reader(csvfile))[0]:
                    is_new = False
                else:
                    is_new = True
        except:
            is_new = True

        with open(path_to_csv, 'ab') as csvfile:
            statswriter = csv.writer(csvfile, delimiter=',')
            # write column names if new file
            if is_new:
                statswriter.writerow(sorted(stats_dict.keys()))
            row_to_write = []
            # write stats row sorted by column name
            for key in sorted(stats_dict.keys()):
                row_to_write.append(stats_dict[key])
            statswriter.writerow(row_to_write)

    def get_stats(self):
        """
        Get basic statistics of the GTFS data.

        Returns
        -------
        stats: dict
            A dictionary of various statistics.
            Keys should be strings, values should be inputtable to a database (int, date, str, ...)
            (but not a list)
        """
        conn = self.conn
        cur = self.conn.cursor()
        stats = {}
        # Basic table counts
        for table in ['agencies', 'routes', 'stops', 'stop_times', 'trips', 'calendar', 'shapes', 'calendar_dates', 'days',
                      'stop_distances', 'frequencies', 'feed_info', 'transfers']:
            n = conn.execute('SELECT count(*) FROM %s' % table).fetchone()[0]
            stats["n_" + table] = n

        # Agency names
        stats["agencies"] = "_".join([x[0] for x in conn.execute('SELECT name FROM agencies').fetchall()]).encode('utf-8')

        # Stop lat/lon range
        lats = [x[0] for x in conn.execute('SELECT lat FROM stops').fetchall()]
        lons = [x[0] for x in conn.execute('SELECT lon FROM stops').fetchall()]
        percentiles = [0, 10, 50, 90, 100]
        lat_min, lat_10, lat_median, lat_90, lat_max = numpy.percentile(lats, percentiles)
        stats["lat_min"] = lat_min
        stats["lat_10"] = lat_10
        stats["lat_median"] = lat_median
        stats["lat_90"] = lat_90
        stats["lat_max"] = lat_max

        lon_min, lon_10, lon_median, lon_90, lon_max = numpy.percentile(lons, percentiles)
        stats["lon_min"] = lon_min
        stats["lon_10"] = lon_10
        stats["lon_median"] = lon_median
        stats["lon_90"] = lon_90
        stats["lon_max"] = lon_max

        stats["height"] = wgs84_distance(lat_min, lon_median, lat_max, lon_median) / 1000.
        stats["width"] = wgs84_distance(lon_min, lat_median, lon_max, lat_median) / 1000.

        first_day_start_ut, last_day_start_ut = \
            cur.execute("SELECT min(day_start_ut), max(day_start_ut) FROM days;").fetchone()

        stats["start_time_ut"] = first_day_start_ut
        if last_day_start_ut is None:
            stats["end_time_ut"] = None
        else:
            # 28 (instead of 24) comes from the GTFS standard
            stats["end_time_ut"] = last_day_start_ut + 28 * 3600

        stats["start_date"] = cur.execute("SELECT min(date) FROM days").fetchone()[0]
        stats["end_date"] = cur.execute("SELECT max(date) FROM days").fetchone()[0]

        # Maximum activity day
        max_activity_date = cur.execute(
            'SELECT count(*), date FROM days GROUP BY date '
            'ORDER BY count(*) DESC, date LIMIT 1;').fetchone()
        if max_activity_date:
            stats["max_activity_date"] = max_activity_date[1]
            max_activity_hour = cur.execute(
                'SELECT count(*), arr_time_hour FROM day_stop_times '
                'WHERE date=? GROUP BY arr_time_hour '
                'ORDER BY count(*) DESC;', (stats["max_activity_date"],)).fetchone()
            if max_activity_hour:
                stats["max_activity_hour"] = max_activity_hour[1]
            else:
                stats["max_activity_hour"] = None
        # Fleet size estimate: considering each line separately
        fleet_size_list = []
        for row in cur.execute('Select type, max(vehicles) from '
            '(select type, direction_id, sum(vehicles) as vehicles from '
            '(select trips.route_I, trips.direction_id, routes.route_id, name, type, count(*) as vehicles, cycle_time_min from trips, routes, days, '
            '(select first_trip.route_I, first_trip.direction_id, first_trip_start_time, first_trip_end_time, '
            'MIN(start_time_ds) as return_trip_start_time, end_time_ds as return_trip_end_time, '
            '(end_time_ds - first_trip_start_time)/60 as cycle_time_min from trips, '
            '(select route_I, direction_id, MIN(start_time_ds) as first_trip_start_time, end_time_ds as first_trip_end_time from trips, days '
            'where trips.trip_I=days.trip_I and start_time_ds >= ? * 3600 and start_time_ds <= (? + 1) * 3600 and date = ? '
            'group by route_I, direction_id) first_trip '
            'where first_trip.route_I = trips.route_I and first_trip.direction_id != trips.direction_id and start_time_ds >= first_trip_end_time '
            'group by trips.route_I, trips.direction_id) return_trip '
            'where trips.trip_I=days.trip_I and trips.route_I= routes.route_I and date = ? and trips.route_I = return_trip.route_I and trips.direction_id = return_trip.direction_id and start_time_ds >= first_trip_start_time and start_time_ds < return_trip_end_time '
            'group by trips.route_I, trips.direction_id '
            'order by type, name, vehicles desc) cycle_times '
            'group by direction_id, type) vehicles_type '
            'group by type;', (stats["max_activity_hour"], stats["max_activity_hour"], stats["max_activity_date"], stats["max_activity_date"])):
            fleet_size_list.append(str(row[0]) + ':' + str(row[1]))
        stats["fleet_size_route_based"] = ' '.join(fleet_size_list)
        # Fleet size estimate: maximum number of vehicles in movement
        fleet_size_dict = {}
        fleet_size_list = []
        if stats["max_activity_hour"]:
            for minute in range(stats["max_activity_hour"]*3600, (stats["max_activity_hour"]+1)*3600, 60):
                for row in cur.execute('SELECT type, count(*) FROM trips, routes, days '
                    'WHERE trips.route_I = routes.route_I AND trips.trip_I=days.trip_I AND '
                    'start_time_ds <= ? AND end_time_ds > ? + 60 AND date = ? '
                    'GROUP BY type;', (minute, minute, stats["max_activity_date"])):

                    if fleet_size_dict.get(row[0], 0) < row[1]:
                        fleet_size_dict[row[0]] = row[1]

        for key in fleet_size_dict.keys():
            fleet_size_list.append(str(key)+':'+str(fleet_size_dict[key]))
        stats["fleet_size_max_movement"] = ' '.join(fleet_size_list)



        # Compute simple distributions of various colums that have a
        # finite range of values.
        def distribution(table, column):
            """Count occurances of values and return it as a string.

            Example return value:   '1:5 2:15'"""
            cur.execute('SELECT {column}, count(*) '
                        'FROM {table} GROUP BY {column} '
                        'ORDER BY {column}'.format(column=column, table=table))
            return ' '.join('%s:%s'%(t, c) for t, c in cur)\

        # Commented lines refer to values that are not imported yet.
        stats['routes__type__dist'] = distribution('routes', 'type')
        #stats['stop_times__pickup_type__dist'] = distribution('stop_times', 'pickup_type')
        #stats['stop_times__drop_off_type__dist'] = distribution('stop_times', 'drop_off_type')
        #stats['stop_times__timepoint__dist'] = distribution('stop_times', 'timepoint')
        stats['calendar_dates__exception_type__dist'] = distribution('calendar_dates', 'exception_type')
        stats['frequencies__exact_times__dist'] = distribution('frequencies', 'exact_times')
        stats['transfers__transfer_type__dist'] = distribution('transfers', 'transfer_type')
        stats['agencies__lang__dist'] = distribution('agencies', 'lang')
        stats['stops__location_type__dist'] = distribution('stops', 'location_type')
        #stats['stops__wheelchair_boarding__dist'] = distribution('stops', 'wheelchair_boarding')
        #stats['trips__wheelchair_accessible__dist'] = distribution('trips', 'wheelchair_accessible')
        #stats['trips__bikes_allowed__dist'] = distribution('trips', 'bikes_allowed')
        #stats[''] = distribution('', '')
        return stats

    def get_median_lat_lon_of_stops(self):
        """
        Get median latitude and longitude of stops

        Returns
        -------
        median_lat : float
        median_lon : float
        """
        cur = self.conn.cursor()
        lats = [x[0] for x in cur.execute('SELECT lat FROM stops').fetchall()]
        lons = [x[0] for x in cur.execute('SELECT lon FROM stops').fetchall()]
        median_lat = numpy.percentile(lats, 50)
        median_lon = numpy.percentile(lons, 50)
        # {"lat_median": median_lat, "lon_median": median_lon}
        return median_lat, median_lon

    def get_centroid_of_stops(self):
        """
        Get mean latitude and longitude of stops

        Returns
        -------
        mean_lat : float
        mean_lon : float
        """
        cur = self.conn.cursor()
        lats = [x[0] for x in cur.execute('SELECT lat FROM stops').fetchall()]
        lons = [x[0] for x in cur.execute('SELECT lon FROM stops').fetchall()]
        mean_lat = numpy.mean(lats)
        mean_lon = numpy.mean(lons)
        return mean_lat, mean_lon

    def get_conservative_gtfs_time_span_in_ut(self):
        """
        Return conservative estimates of start_time_ut and end_time_uts.
        All trips, events etc. should start after start_time_ut_conservative and end before end_time_ut_conservative

        Returns
        -------
        start_time_ut_conservative : int
        end_time_ut_conservative : int
        """
        # this could be included in the stats?
        cur = self.conn.cursor()
        first_day_start_ut, last_day_start_ut = \
            cur.execute("SELECT min(day_start_ut), max(day_start_ut) FROM days;").fetchone()
        # no trip should start before the first_day_start_ut
        start_time_ut_conservative = first_day_start_ut
        # 28 (instead of 24) comes from the GTFS standard
        end_time_ut_conservative  = last_day_start_ut + 28 * 3600
        return start_time_ut_conservative, end_time_ut_conservative

    def validate_gtfs(self):
        """
        Used for validation of GTFS object. Creates warnings if the feed contains:
        Long Stop Spacings
        5 Or More Consecutive Stop Times With Same Time
        Long Trip Times
        Unrealistic Average Speed (of trip)
        Long Travel Time Between Consecutive Stops

        The warnings dictionary contains the number of each warning type
        The warnings_record collects the rows that produced the warnings

        Returns
        -------
        warnings: dict
        key: "warning type" string, value: "number of errors" int
        warnings_record: dict
        key: "row that produced error" tuple, value: "warning type(s)" string

        """
        # These are the mode - feasable speed combinations used here:
        # https://support.google.com/transitpartners/answer/1095482?hl=en
        type_speed = {
            0: ("Tram", 100),
            1: ("Subway", 150),
            2: ("Rail", 300),
            3: ("Bus", 100),
            4: ("Ferry", 80),
            5: ("Cable Car", 50),
            6: ("Gondola", 50),
            7: ("Funicular", 50)
        }
        max_stop_spacing = 20000  # meters
        max_time_between_stops = 1800  # seconds
        n_stops_with_same_time = 5
        max_trip_time = 7200  # seconds

        conn = self.conn

        conn.create_function("find_distance", 4, wgs84_distance)
        cur = conn.cursor()
        warnings_record = {}
        warnings = {"Long Stop Spacing": 0, "5 Or More Consecutive Stop Times With Same Time": 0, "Long Trip Time": 0,
                    "Unrealistic Average Speed": 0, "Long Travel Time Between Consecutive Stops": 0}

        # this query calculates distance and travel time between consecutive stops
        for n in cur.execute('select q1.trip_I, type, q1.stop_I as stop_1, q2.stop_I as stop_2, '
                             'CAST(find_distance(q1.lat, q1.lon, q2.lat, q2.lon) AS INT) as distance, '
                             'q2.arr_time_ds - q1.arr_time_ds as traveltime '
                             'from (select * from stop_times, '
                             'stops where stop_times.stop_I = stops.stop_I) q1, (select * from stop_times, '
                             'stops where stop_times.stop_I = stops.stop_I) q2, trips, routes where q1.trip_I = q2.trip_I '
                             'and q1.seq + 1 = q2.seq and q1.trip_I = trips.trip_I and trips.route_I = routes.route_I ').fetchall():

            if n[4] > max_stop_spacing:
                warnings_record[n] = "Long Stop Spacing"
                warnings["Long Stop Spacing"] += 1
            if n[5] > max_time_between_stops:

                warnings["Long Travel Time Between Consecutive Stops"] += 1
                if n in warnings_record.keys():
                    warnings_record[n] += "," + "Long Travel Time Between Consecutive Stops"
                else:
                    warnings_record[n] = "Long Travel Time Between Consecutive Stops"

        # this query returns the trips where there are N or more stops with the same stop time
        for n in cur.execute('select trip_I, arr_time, N from ( select trip_I, arr_time, count(*) as N '
                             'from stop_times group by trip_I, arr_time) q1 where N >= ?', (n_stops_with_same_time,)):
            warnings["5 Or More Consecutive Stop Times With Same Time"] += 1
            warnings_record[n] = "5 Or More Consecutive Stop Times With Same Time"

        # this query calculates the distance and travel time for each complete trip
        for n in cur.execute(
                'select q1.trip_I, type, '
                'sum(CAST(find_distance(q1.lat, q1.lon, q2.lat, q2.lon) AS INT)) as total_distance, '
                'sum(q2.arr_time_ds - q1.arr_time_ds) as total_traveltime '
                'from (select * from stop_times, '
                'stops where stop_times.stop_I = stops.stop_I) q1, (select * from stop_times, '
                'stops where stop_times.stop_I = stops.stop_I) q2, trips, routes where q1.trip_I = q2.trip_I '
                'and q1.seq + 1 = q2.seq and q1.trip_I = trips.trip_I and trips.route_I = routes.route_I group by q1.trip_I').fetchall():

            avg_velocity = n[2] / n[3] * 3.6
            if avg_velocity > type_speed[n[1]][1]:
                warnings["Unrealistic Average Speed"] += 1
                warnings_record[n] = "Unrealistic Average Speed"

            if n[3] > max_trip_time:
                warnings["Long Trip Time"] += 1
                if n in warnings_record.keys():
                    warnings_record[n] += "," + "Long Trip Time"
                else:
                    warnings_record[n] = "Long Trip Time"

        for key in warnings.keys():
            print key + ": " + str(warnings[key])

        return warnings, warnings_record

class GTFSMetadata(object):
    """
    This provides dictionary protocol for metadata.

    TODO: does not rep ???
    """

    def __init__(self, conn):
        self._conn = conn

    def __getitem__(self, key):
        val = self._conn.execute('SELECT value FROM metadata WHERE key=?',
                                 (key,)).fetchone()
        if not val:
            raise KeyError("This GTFS does not have metadata: %s" % key)
        return val[0]

    def __setitem__(self, key, value):
        """Get metadata from the DB"""
        if isinstance(value, binary_type):
            value = value.decode('utf-8')
        ret = self._conn.execute('INSERT OR REPLACE INTO metadata '
                                 '(key, value) VALUES (?, ?)',
                                 (key, value)).fetchone()
        self._conn.commit()

    def __delitem__(self, key):
        self._conn.execute('DELETE FROM metadata WHERE key=?',
                           (key,)).fetchone()
        self._conn.commit()

    def __iter__(self):
        cur = self._conn.execute('SELECT key FROM metadata ORDER BY key')
        return (x[0] for x in cur)

    def __contains__(self, key):
        val = self._conn.execute('SELECT value FROM metadata WHERE key=?',
                                 (key,)).fetchone()
        return val is not None

    def get(self, key, default=None):
        val = self._conn.execute('SELECT value FROM metadata WHERE key=?',
                                 (key,)).fetchone()
        if not val:
            return default
        return val[0]

    def items(self):
        cur = self._conn.execute('SELECT key, value FROM metadata ORDER BY key')
        return cur

    def update(self, dict_):
        # Would be more efficient to do it in a new query here, but
        # prefering simplicity.  metadata updates are probably
        # infrequent.
        if hasattr(dict_, 'items'):
            for key, value in dict_.items():
                self[key] = value
        else:
            for key, value in dict_:
                self[key] = value


if __name__ == "__main__":
    cmd = sys.argv[1]
    args = sys.argv[2:]
    if cmd == 'stats':
        print args[0]
        G = GTFS(args[0])
        G.calc_and_store_stats()
        for row in G.meta.items():
            print row
    elif cmd == 'metadata-list':
        #print args[0]  # need to not print to be valid json on stdout
        G = GTFS(args[0])
        #for row in G.meta.items():
        #    print row
        stats = dict(G.meta.items())
        import json
        print json.dumps(stats, sort_keys=True,
                         indent=4, separators=(',', ': '))
    elif cmd == 'make-daily':
        from_db = args[0]
        g = GTFS(from_db)
        to_db = args[1]
        download_date = g.meta['download_date']
        d = datetime.datetime.strptime(download_date, '%Y-%m-%d').date()
        date_start = d + datetime.timedelta(7 - d.isoweekday() + 1)      # inclusive
        date_end   = d + datetime.timedelta(7 - d.isoweekday() + 1 + 1)  # exclusive
        g.copy_and_filter(to_db, start_date=date_start, end_date=date_end)
    elif cmd == 'make-weekly':
        from_db = args[0]
        g = GTFS(from_db)
        to_db = args[1]
        download_date = g.meta['download_date']
        d = datetime.datetime.strptime(download_date, '%Y-%m-%d').date()
        date_start = d + datetime.timedelta(7 - d.isoweekday() + 1)  # inclusive
        date_end = d + datetime.timedelta(7 - d.isoweekday() + 1 + 7)  # exclusive
        print date_start, date_end
        g.copy_and_filter(to_db, start_date=date_start, end_date=date_end)
    elif "spatial-extract":
        try:
            from_db = args[0]
            lat = float(args[1])
            lon = float(args[2])
            radius_in_km = float(args[3])
            to_db = args[4]
        except Exception as e:
            print "spatial-extract usage: python gtfs.py spatial-extract fromdb.sqlite center_lat center_lon " \
                  "radius_in_km todb.sqlite"
            raise e
        logging.basicConfig(level=logging.INFO)
        logging.info("Loading initial database")
        g = GTFS(from_db)
        g.copy_and_filter(to_db, buffer_distance=radius_in_km * 1000, buffer_lat=lat, buffer_lon=lon)
    elif cmd == 'interact':
        G = GTFS(args[0])
        import IPython
        IPython.embed()
    else:
        print("Unrecognized command: %s" % cmd)
        exit(1)
