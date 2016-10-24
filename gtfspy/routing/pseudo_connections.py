from gtfspy.routing.models import Connection


def compute_pseudo_connections(transit_connections, start_time_dep,
                               end_time_dep, transfer_margin,
                               walk_network, walk_speed):
    """
    Given a set of transit events and the static walk network,
    "transform" the static walking network into a set of "pseudo-connections".

    As a first approximation, we add pseudo-connections to depart after each arrival of a transit connection
    to it's arrival stop.

    Parameters
    ----------
    transit_connections: list[Connection]
    start_time_dep : int
        start time in unixtime seconds
    end_time_dep: int
        end time in unixtime seconds (no new connections will be scanned after this time)
    transfer_margin: int
        required extra margin required for transfers in seconds
    walk_speed: float
        walking speed between stops in meters / second
    walk_network: networkx.Graph
        each edge should have the walking distance as a data attribute ("d_walk") expressed in meters

    Returns
    -------
    pseudo_connections: list[Connection]
    """
    # A pseudo-connection should be created after (each) arrival to a transit_connection's arrival stop.
    pseudo_connection_set = set()  # use a set to ignore possible duplicates
    for c in transit_connections:
        if start_time_dep <= c.departure_time <= end_time_dep:
            walk_dep_stop = c.arrival_stop
            walk_dep_time = c.arrival_time
            if walk_dep_time > end_time_dep:
                continue
            for _, walk_arr_stop, data in walk_network.edges(nbunch=[walk_dep_stop], data=True):
                walk_arr_time = walk_dep_time + data['d_walk'] / float(walk_speed)
                pseudo_connection = Connection(departure_stop=walk_dep_stop,
                                               arrival_stop=walk_arr_stop,
                                               departure_time=walk_dep_time,
                                               arrival_time=walk_arr_time,
                                               trip_id=None,
                                               is_walk=True)
                pseudo_connection_set.add(pseudo_connection)
    return list(pseudo_connection_set)



