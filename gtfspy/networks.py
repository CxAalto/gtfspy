from __future__ import print_function

import sys
import networkx

import pandas as pd
import numpy

from gtfspy.util import wgs84_distance
from gtfspy import route_types


def walk_stop_to_stop_network(gtfs):
    """
    Get the walk network.
    If OpenStreetMap-based walking distances have been computed, then those are used as the distance.
    Otherwise, the great circle distances ("d_great_circle") is used.

    Parameters
    ----------
    gtfs: gtfspy.GTFS

    Returns
    -------
    net: networkx.DiGraph
    """
    net = networkx.Graph()
    _add_stops_to_net(net, gtfs.get_table("stops"))
    transfers = gtfs.get_table("stop_distances")
    for transfer in transfers.itertuples():
        from_node = transfer.from_stop_I
        to_node = transfer.to_stop_I
        d = transfer.d
        d_walk = transfer.d_walk
        net.add_edge(from_node, to_node, {"d_great_circle": d, "d_shape": d_walk})
    return net


def write_transit_stop_to_stop_network_edgelist(gtfs, filename_to_write):
    """
    Write the

    Parameters
    ----------
    gtfs : gtfspy.GTFS
    filename_to_write : str

    Returns
    -------
    None
    """
    net = transit_stop_to_stop_network_directed(gtfs)
    networkx.write_edgelist(net, filename_to_write, delimiter=",", data=True)


def aggregate__network(self, graph, distance):
    """
    See to_aggregate_line_graph for documentation
    """
    raise NotImplementedError("this is not working fully yet")
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


def route_type_stop_to_stop_network(gtfs,
                                    route_type,
                                    link_attributes=None,
                                    start_time_ut=None,
                                    end_time_ut=None):
    """
    Get a stop-to-stop network describing a single mode of travel.

    Parameters
    ----------
    gtfs : gtfspy.GTFS
    route_type : int
        See gtfspy.route_types for the list of possible types.
    link_attributes: list[str], optional
        defaulting to use the following link attributes:
            "n_vehicles" : Number of vehicles passed
            "duration_min" : minimum travel time between stops
            "duration_max" : maximum travel time between stops
            "duration_median" : median travel time between stops
            "duration_avg" : average travel time between stops
            "d_great_circle" : distance along straight line (wgs84_distance)
            "distance_shape" : minimum distance along shape
            "capacity_estimate"  : approximate capacity passed through the stop
            "route_ids" : route id
    start_time_ut
        start time of the time span (in unix time)
    end_time_ut: int
        end time of the time span (in unix time)

    Returns
    -------
    net: networkx.DiGraph
        A directed graph Directed graph

    """
    if route_type is route_types.WALK:
        net = walk_stop_to_stop_network(gtfs)
        for from_node, to_node, data in net.edges(data=true):
            data["n_vehicles"] = None
            data["duration_min"] = None
            data["duration_max"] = None
            data["duration_avg"] = None
            data["duration_median"] = None
            data["capacity_estimate"] = None
            data["route_ids"] = None
        return net.to_directed()
    else:
        stops_dataframe = gtfs.get_stops_for_route_type(route_type)
        net = networkx.DiGraph()
        _add_stops_to_net(net, stops_dataframe)

        events_df = gtfs.get_transit_events(start_time_ut=start_time_ut,
                                            end_time_ut=end_time_ut,
                                            route_type=route_type)

        # group events by links, and loop over them (i.e. each link):
        link_event_groups = events_df.groupby(['from_stop_I', 'to_stop_I'], sort=False)
        for key, link_events in link_event_groups:
            from_stop_I, to_stop_I = key
            assert isinstance(link_events, pd.DataFrame)
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
                    link_data['capacity_estimate'] = route_types.ROUTE_TYPE_TO_APPROXIMATE_CAPACITY[route_type]
                if "distance_great_circle" in link_attributes:
                    from_lat = net.node[from_stop_I]['lat']
                    from_lon = net.node[from_stop_I]['lon']
                    to_lat = net.node[to_stop_I]['lat']
                    to_lon = net.node[to_stop_I]['lon']
                    distance = wgs84_distance(from_lat, from_lon, to_lat, to_lon)
                    link_data['distance_great_circle'] = distance
                if "distance_shape" in link_attributes:
                    assert "shape_id" in link_events.columns.values
                    found = None
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
        return net


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
    ?
    """
    pass


def aggregate_stop_to_stop_network(network, distance):
    """
    Aggregate graph by grouping nodes that are within a specified distance.
    The ids of the nodes are tuples of the original stop_Is.

    Parameters
    ----------
    network: networkx.DiGraph
    distance: float
        group all nodes within this distance.

    Returns
    -------
    graph: networkx.Graph
    """
    pass


def main():
    pass

if __name__ == "__main__":
    main()


# def undirected_stop_to_stop_network_with_route_information(gtfs, verbose=True):
#     """
#     Return a graph, where edges have route_id as labels.
#     Note: Only one route variation of each route is taken into account.
#     Un-connected stops are filtered out from the network.
#
#     Returns
#     -------
#     giant: networkx.Graph
#         the largest connected component of the undirected line graph
#
#     """
#     net = networkx.Graph()
#     node_data_frame = gtfs.stops()
#     for stop_tuple in node_data_frame.itertuples(index=False, name="NamedTupleStop"):
#         node_attributes = {
#             "lat": stop_tuple.lat,
#             "lon": stop_tuple.lon,
#             "name": stop_tuple.name,
#         }
#         net.add_node(stop_tuple.stop_I, attr_dict=node_attributes)
#
#     rows = gtfs.conn.cursor().execute(
#         "SELECT trip_I, route_I, route_id "
#         "FROM routes "
#         "LEFT JOIN trips "
#         "USING(route_I) "
#         "GROUP BY route_I").fetchall()
#
#     # Grouping by route_I to consider only one route variation per route
#     # (looping over all trip_Is would be too costly)
#
#     for trip_I, route_I, route_id in rows:
#         if trip_I is None:
#             continue
#         query2 = "SELECT stop_I, seq " \
#                  "FROM stop_times " \
#                  "WHERE trip_I={trip_I} " \
#                  "ORDER BY seq".format(trip_I=trip_I)
#         df = pd.read_sql(query2, gtfs.conn)
#         stop_Is = df['stop_I'].values
#         edges = zip(stop_Is[:-1], stop_Is[1:])
#         for from_stop_I, to_stop_I in edges:
#             if net.has_edge(from_stop_I, to_stop_I):
#                 edge_data = net.get_edge_data(from_stop_I, to_stop_I)
#                 edge_data["route_ids"].append(route_id)
#                 edge_data["route_Is"].append(route_I)
#             else:
#                 net.add_edge(from_stop_I, to_stop_I,
#                              route_ids=[route_id], route_Is=[route_I])
#     if verbose:
#         if len(net.edges()) == 0:
#             print("Warning: no edges in the line network, is the stop_times table defined properly?")
#
#     # return only the maximum connected component to remove unassosicated nodes
#     giant = max(networkx.connected_component_subgraphs(net), key=len)
#     return giant


def _add_stops_to_net(net, stops):
    """
    Add nodes to the network from the pandas dataframe describing (a part of the) stops table in the GTFS database.

    Parameters
    ----------
    net: networkx.Graph
    stops: pandas.DataFrame
    """
    for stop in stops.itertuples():
        data = {
            "lat" : stop.lat,
            "lon" : stop.lon,
            "name" : stop.name
        }
        net.add_node(stop.stop_I, data)

