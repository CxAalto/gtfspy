import os
from warnings import warn

import networkx
from geoindex import GeoGridIndex, GeoPoint
from osmread import parse_file, Way, Node

from gtfspy.gtfs import GTFS
from gtfspy.util import wgs84_distance


def add_walk_distances_to_db_python(gtfs, osm_path, cutoff_distance_m=1000):
    """
    Computes the walk paths between stops, and updates these to the gtfs database.

    Parameters
    ----------
    gtfs: gtfspy.GTFS or str
        A GTFS object or a string representation.
    osm_path: str
        path to the OpenStreetMap file
    cutoff_distance_m: number
        maximum allowed distance in meters

    Returns
    -------
    None

    See Also
    --------
    gtfspy.calc_transfers
    compute_walk_paths_java
    """
    if isinstance(gtfs, str):
        gtfs = GTFS(gtfs)
    assert isinstance(gtfs, GTFS)
    print("Reading in walk network")
    walk_network = create_walk_network_from_osm(osm_path)
    print("Matching stops to the OSM network")
    stop_I_to_nearest_osm_node, stop_I_to_nearest_osm_node_distance = match_stops_to_nodes(
        gtfs, walk_network
    )

    transfers = gtfs.get_straight_line_transfer_distances()

    from_I_to_to_stop_Is = {stop_I: set() for stop_I in stop_I_to_nearest_osm_node}
    for transfer_tuple in transfers.itertuples():
        from_I = transfer_tuple.from_stop_I
        to_I = transfer_tuple.to_stop_I
        from_I_to_to_stop_Is[from_I].add(to_I)

    print("Computing walking distances")
    for from_I, to_stop_Is in from_I_to_to_stop_Is.items():
        from_node = stop_I_to_nearest_osm_node[from_I]
        from_dist = stop_I_to_nearest_osm_node_distance[from_I]
        shortest_paths = networkx.single_source_dijkstra_path_length(
            walk_network, from_node, cutoff=cutoff_distance_m - from_dist, weight="distance"
        )
        for to_I in to_stop_Is:
            to_distance = stop_I_to_nearest_osm_node_distance[to_I]
            to_node = stop_I_to_nearest_osm_node[to_I]
            osm_distance = shortest_paths.get(to_node, float("inf"))
            total_distance = from_dist + osm_distance + to_distance
            from_stop_I_transfers = transfers[transfers["from_stop_I"] == from_I]
            straigth_distance = from_stop_I_transfers[from_stop_I_transfers["to_stop_I"] == to_I][
                "d"
            ].values[0]
            assert (
                straigth_distance < total_distance + 2
            )  # allow for a maximum  of 2 meters in calculations
            if total_distance <= cutoff_distance_m:
                gtfs.conn.execute(
                    "UPDATE stop_distances "
                    "SET d_walk = "
                    + str(int(total_distance))
                    + " WHERE from_stop_I="
                    + str(from_I)
                    + " AND to_stop_I="
                    + str(to_I)
                )

    gtfs.conn.commit()


def match_stops_to_nodes(gtfs, walk_network):
    """
    Parameters
    ----------
    gtfs : a GTFS object
    walk_network : networkx.Graph

    Returns
    -------
    stop_I_to_node: dict
        maps stop_I to closest walk_network node
    stop_I_to_dist: dict
        maps stop_I to the distance to the closest walk_network node
    """
    network_nodes = walk_network.nodes(data="true")

    stop_Is = set(gtfs.get_straight_line_transfer_distances()["from_stop_I"])
    stops_df = gtfs.stops()

    geo_index = GeoGridIndex(precision=6)
    for net_node, data in network_nodes:
        geo_index.add_point(GeoPoint(data["lat"], data["lon"], ref=net_node))
    stop_I_to_node = {}
    stop_I_to_dist = {}
    for stop_I in stop_Is:
        stop_lat = float(stops_df[stops_df.stop_I == stop_I].lat)
        stop_lon = float(stops_df[stops_df.stop_I == stop_I].lon)
        geo_point = GeoPoint(stop_lat, stop_lon)
        min_dist = float("inf")
        min_dist_node = None
        search_distances_m = [0.100, 0.500]
        for search_distance_m in search_distances_m:
            for point, distance in geo_index.get_nearest_points(geo_point, search_distance_m, "km"):
                if distance < min_dist:
                    min_dist = distance * 1000
                    min_dist_node = point.ref
            if min_dist_node is not None:
                break
        if min_dist_node is None:
            warn("No OSM node found for stop: " + str(stops_df[stops_df.stop_I == stop_I]))
        stop_I_to_node[stop_I] = min_dist_node
        stop_I_to_dist[stop_I] = min_dist
    return stop_I_to_node, stop_I_to_dist


OSM_HIGHWAY_WALK_TAGS = {
    "trunk",
    "trunk_link",
    "primary",
    "primary_link",
    "secondary",
    "secondary_link",
    "tertiary",
    "tertiary_link",
    "unclassified",
    "residential",
    "living_street",
    "road",
    "pedestrian",
    "path",
    "cycleway",
    "footway",
}


def create_walk_network_from_osm(osm_file):
    walk_network = networkx.Graph()
    assert os.path.exists(osm_file)
    ways = []
    for i, entity in enumerate(parse_file(osm_file)):
        if isinstance(entity, Node):
            walk_network.add_node(entity.id, lat=entity.lat, lon=entity.lon)
        elif isinstance(entity, Way):
            if "highway" in entity.tags:
                if entity.tags["highway"] in OSM_HIGHWAY_WALK_TAGS:
                    ways.append(entity)
    for way in ways:
        walk_network.add_path(way.nodes)
    del ways

    # Remove all singleton nodes (note that taking the giant component does not necessarily provide proper results.
    for node, degree in walk_network.degree().items():
        if degree is 0:
            walk_network.remove_node(node)

    node_lats = networkx.get_node_attributes(walk_network, "lat")
    node_lons = networkx.get_node_attributes(walk_network, "lon")
    for source, dest, data in walk_network.edges(data=True):
        data["distance"] = wgs84_distance(
            node_lats[source], node_lons[source], node_lats[dest], node_lons[dest]
        )
    return walk_network


def compute_walk_paths_java(gtfs_db_path, osm_file, cache_db=None):
    """
    Parameters
    ----------
    gtfs_db_path: str (path to the gtfs database)
    osm_file: str
    cache_db: str

    Returns
    -------
    None
    """
    raise NotImplementedError("This has not been pipelined yet. Please see the Python code.")
