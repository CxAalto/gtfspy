import networkx


def aggregate_stops_spatially(gtfs, threshold_meters=1):
    """
    Aggregate stops spatially based on straight-line distances.
    Adjusts the underlying GTFS-database.

    Parameters
    ----------
    gtfs: gtfspy.GTFS
    threshold_meters: int, optional
    """
    # Get all stops that are not any other node's parent_stop_I
    non_parent_stops_df = gtfs.stops(exclude_parent_stops=True)

    # Create a network of those stops, with links between nodes that are less than threshold_m apart from each other.
    g = networkx.Graph()
    for stop_I in list(non_parent_stops_df['stop_I'].values):
        g.add_node(stop_I)

    stop_I_list_str = "(" + ",".join([str(stop_I) for stop_I in non_parent_stops_df['stop_I'].values]) + ")"
    stop_distances_sql = "SELECT * from STOP_DISTANCES WHERE from_stop_I IN " + stop_I_list_str + \
                         " AND to_stop_I IN " + stop_I_list_str + " AND d<" + str(threshold_meters)
    stop_distances_df = gtfs.execute_custom_query_pandas(stop_distances_sql)
    for row in stop_distances_df.itertuples():
        g.add_edge(row.from_stop_I, row.to_stop_I)

    # Find the connected components of that network.
    components = list(networkx.connected_components(g))
    print([len(component) for component in components])

    # For each connected component, choose one representative stop_I, and create a dictionary `to_new_stop_I' mapping original stop_I to the representative stop_I
    to_new_stop_I = dict()
    for component in components:
        component_iter = iter(component)
        representative_stop_I = next(component_iter)
        for stop_I in component_iter:
            to_new_stop_I[stop_I] = representative_stop_I

    # Remove those stops in `stops` table that are not mapped to themselves in `to_new_stop_I'.
    stop_Is_to_remove = []
    for key, value in to_new_stop_I.items():
        if key != value:
            stop_Is_to_remove.append(key)

    stop_Is_to_remove_list_str = "(" + ",".join([str(stop_I) for stop_I in stop_Is_to_remove]) + ")"

    # Remove rows from stops and stop_distances tables where
    print("    Removing ", len(stop_Is_to_remove), " duplicate stops")
    gtfs.execute_custom_query("DELETE FROM stops WHERE stop_I IN " + stop_Is_to_remove_list_str)
    gtfs.execute_custom_query("DELETE FROM stop_distances WHERE " +
                              "from_stop_I IN " + stop_Is_to_remove_list_str +
                              "OR to_stop_I IN " + stop_Is_to_remove_list_str)

    # Replace all stop_I_values of stop_times with
    to_change = []
    for old_stop_I, new_stop_I in to_new_stop_I.items():
        if old_stop_I != new_stop_I:
            to_change.append((int(new_stop_I), int(old_stop_I)))
    # create a temporary index if there are a lot of changes
    if len(to_change) > 10:
        gtfs.conn.execute("CREATE INDEX IF NOT EXISTS tmp_index_stop_times_stop_I ON stop_times (stop_I)")
    # Do the actual updates
    gtfs.conn.executemany("UPDATE stop_times SET stop_I=? WHERE stop_I=?", to_change)
    # Remove the temporary index
    gtfs.conn.execute("DROP INDEX IF EXISTS tmp_index_stop_times_stop_I")
    gtfs.conn.commit()
