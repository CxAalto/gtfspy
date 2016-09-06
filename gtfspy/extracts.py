import sys
import networkx

import pandas as pd
import numpy

from gtfspy.util import wgs84_distance

def stop_to_stop_network(gtfs):
    """
    First priority:
        raw data, individual stops, directed
    Link attributes:
        From node
        To node
        Number of vehicles passed
        Approximate capacity passed
        Average travel time between stops
        Straight-line distance
        List of lines, separated with a
        Node attributes:
        ID
        Coordinates
        Name of the stop
        Data format to be used:
        Edge file (i, j, vehicle count, capacity, travel time, distance)
    """
    pass


def multi_layer_network(gtfs):
    """
    Stop-to-stop networks + layers reflecting modality
        Ask Mikko for more details?
        Separate networks for each mode.
        Modes:
        Walking + GTFS

    Parameters
    ----------
    gtfs

    Returns
    -------
    ?

    """
    pass


def multilayer_temporal_network(gtfs):
    """
    Parameters
    ----------
    gtfs

    Returns
    -------
    multilayer_temporal_network? :)
    """
    pass


def _undir_line_graph_to_aggregated(self, graph, distance):
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
            "lat": new_lat,
            "lon": new_lon,
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


def aggregate_line_network(gtfs, distance):
    """
    Aggregate graph by grouping nodes that are within
    a specified distance.
    The ids of the nodes are tuples of the original stop_Is.


    For more details see to_undirected_line_graph()

    Parameters
    ----------
    gtfs: gtfspy.GTFS
    distance: float
        group all nodes within this distance.

    Returns
    -------
    graph: networkx.Graph
    """
    graph = undirected_line_network(gtfs)
    return _undir_line_graph_to_aggregated(gtfs, graph, distance)


def undirected_line_network(gtfs, verbose=True):
    """
    Return a graph, where edges have route_id as labels.
    Only one arbitrary "instance/trip" of each "route"
    (or "line") is taken into account.
    Non-connected stops are filtered out from the network.

    Returns
    -------
    giant: networkx.Graph
        the largest connected component of the undirected line graph

    """
    net = networkx.Graph()
    nodeDataFrame = gtfs.get_stop_info()  # node data frame
    for stopTuple in nodeDataFrame.itertuples(index=False, name="NamedTupleStop"):
        node_attributes = {
            "lat": stopTuple.lat,
            "lon": stopTuple.lon,
            "name": stopTuple.name,
        }
        net.add_node(stopTuple.stop_I, attr_dict=node_attributes)

    rows = gtfs.conn.cursor().execute(
        "SELECT trip_I, route_I, route_id "
        "FROM routes "
        "LEFT JOIN trips "
        "USING(route_I) "
        "GROUP BY route_I").fetchall()
    # Grouping by route_I to consider only one route variation per route
    # (looping over all trip_Is would be too costly)

    for trip_I, route_I, route_id in rows:
        if trip_I is None:
            continue
        query2 = "SELECT stop_I, seq " \
                 "FROM stop_times " \
                 "WHERE trip_I={trip_I} " \
                 "ORDER BY seq".format(trip_I=trip_I)
        df = pd.read_sql(query2, gtfs.conn)
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


def directed_stop_to_stop_network(gtfs, link_attributes=None,
                                  start_time_ut=None, end_time_ut=None):

    """
    Get a static graph presentation (networkx graph) of the GTFS feed.
    Node indices correspond to integers (stop_I's in the underlying GTFS database).

    Parameters
    ----------
    gtfs : gtfspy.GTFS
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
    nodeDataFrame = gtfs.get_stop_info()  # node data frame
    for stopTuple in nodeDataFrame.itertuples(index=False, name="NamedTupleStop"):
        node_attributes = {
            "lat": stopTuple.lat,
            "lon": stopTuple.lon,
            "name": stopTuple.name,
        }
        net.add_node(stopTuple.stop_I, attr_dict=node_attributes)
    n_nodes = len(net.nodes())

    # get all trips
    events_df = gtfs.get_transit_events(start_time_ut=start_time_ut, end_time_ut=end_time_ut)
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
                # TODO !
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
                    distance = gtfs.get_shape_distance_between_stops(
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


def main():
    pass

if __name__ == "__main__":
    main()
