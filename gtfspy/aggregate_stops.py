import networkx
import sqlite3
import pandas as pd
import math
from gtfspy.gtfs import GTFS
from shapely.geometry import Point, MultiPoint
from geopandas import GeoDataFrame, sjoin
from gtfspy.util import get_utm_srid_from_wgs

crs_wgs = {'init': 'epsg:4326'}


def merge_stops_tables(gtfs_self, gtfs_other):
    """
      This function takes an external database, looks of common stops and adds all stops to both databases.
      In addition the stop_pair_I column is added. This id links the stops between these two sources.
      :param source: directory of external database
      :return:
      """
    g_self = GTFS(gtfs_self) #old
    g_other = GTFS(gtfs_other) #lm

    cluster_stops(g_self, g_other, distance=2)

    cur_self = g_self.conn.cursor()
    cur_other = g_other.conn.cursor()

    #g_self.attach_gtfs_database(gtfs_other)

    len_self = len(g_self.execute_custom_query_pandas("SELECT * FROM stops"))
    len_other = len(g_other.execute_custom_query_pandas("SELECT * FROM stops"))

    try:
        g_self.execute_custom_query("""ALTER TABLE stops ADD COLUMN sort_by INT""")

        g_other.execute_custom_query("""ALTER TABLE stops ADD COLUMN sort_by INT""")
    except sqlite3.OperationalError:
        pass

    g_self.execute_custom_query("""UPDATE stops SET sort_by = rowid""")

    g_other.execute_custom_query("""UPDATE stops SET sort_by = rowid+{len_self}""".format(len_self=len_other))
    g_self.conn.commit()
    g_other.conn.commit()


    add_to_other = g_self.execute_custom_query_pandas("SELECT stop_I, stop_id, code, name, desc, lat, lon, "
                                                      "CAST(parent_I AS INT) AS parent_I, "
                                                      "CAST(location_type AS INT) AS location_type,  "
                                                      "CAST(wheelchair_boarding AS INT) AS wheelchair_boarding, "
                                                      "self_or_parent_I, sort_by"
                                                      " FROM stops")

    add_to_self = g_other.execute_custom_query_pandas("SELECT stop_I, stop_id, code, name, desc, lat, lon, "
                                                      "CAST(parent_I AS INT) AS parent_I, "
                                                      "CAST(location_type AS INT) AS location_type,  "
                                                      "CAST(wheelchair_boarding AS INT) AS wheelchair_boarding, "
                                                      "self_or_parent_I, sort_by"
                                                      " FROM stops")

    sort_by_self = "A"
    sort_by_other = "B"
    counter = 0

    rows_to_add_to_self = []
    rows_to_add_to_other = []

    for items in add_to_self.itertuples(index=False):
        rows_to_add_to_self.append((sort_by_other + str(items.stop_id),) +
                                   (str(items.code),
                                    str(items.name),
                                    str(items.desc),
                                    float(items.lat),
                                    float(items.lon),
                                    int(items.location_type),
                                    bool(items.wheelchair_boarding),
                                    int(items.sort_by)))

        counter += 1

    counter = 0
    for items in add_to_other.itertuples(index=False):
        rows_to_add_to_other.append((sort_by_self + str(items.stop_id),) +
                                    (str(items.code),
                                     str(items.name),
                                     str(items.desc),
                                     float(items.lat),
                                     float(items.lon),
                                     int(items.location_type),
                                     bool(items.wheelchair_boarding),
                                     int(items.sort_by)))
        counter += 1

    query_add_row = """INSERT INTO stops(
                                stop_id,
                                code,
                                name,
                                desc,
                                lat,
                                lon,
                                location_type,
                                wheelchair_boarding,
                                sort_by) VALUES (%s) """ % (", ".join(["?" for _ in range(9)]))
    g_self.conn.executemany(query_add_row, rows_to_add_to_self)
    g_other.conn.executemany(query_add_row, rows_to_add_to_other)
    g_self.conn.commit()
    g_other.conn.commit()


def remove_unmatching_stops_multi(gtfs_cons, new_gtfs_paths, max_distance=500):
    from gtfspy.filter import FilterExtract
    print("removing unmatching stops")
    candidate_stops = []
    for gtfs in gtfs_cons:
        all_stops = gtfs.stops()
        active_stops = gtfs.stops(require_reference_in_stop_times=True)

        all_stops_gdf, srid = df_to_utm_gdf(all_stops)
        active_stops_gdf, _ = df_to_utm_gdf(active_stops)
        active_stops_gdf["geometry"] = active_stops_gdf["geometry"].buffer(max_distance)
        active_stops_gdf["everything"] = 1
        active_stops_gdf = active_stops_gdf[["geometry", "everything"]]
        active_stops_gdf = active_stops_gdf.dissolve(by="everything")

        all_stops_gdf = sjoin(all_stops_gdf, active_stops_gdf, how="left", op='within')

        stops_within_threshold_gdf = all_stops_gdf.loc[all_stops_gdf.index_right == 1]
        stops_within_threshold = set(stops_within_threshold_gdf["stop_I"].tolist())
        candidate_stops.append(stops_within_threshold)
    stops_intersection = set.intersection(*candidate_stops)

    stops_union = set.union(*candidate_stops)

    if len(stops_intersection) == len(stops_union):
        print("no stops to remove")
    for gtfs, new_gtfs_path in zip(gtfs_cons, new_gtfs_paths):
        fe = FilterExtract(gtfs, new_gtfs_path, stops_to_keep=stops_intersection,
                           split_trips_partially_outside_buffer=False)
        fe.create_filtered_copy()


def merge_stops_tables_multi(gtfs_cons, threshold_meters=5):
    """
    Takes the stops from multiple feeds and adds everything into a master stop dataframe, with a feed
    identifier column.
    The dataframe is spatially aggregated, pairing all stop_Is + feed identifier with a universal stop_pair_I.
    The stop_pair_I is added to all feed databases.
    The unmatched stops for each DB is also added: stop_pair_Is not present among the original stops of each feed
    stop_Is of the feeds are replaced with stop_pair_I along with all references to stop_I
    """
    print("merging stop tables")
    df = pd.DataFrame()
    for i, gtfs in enumerate(gtfs_cons):
        new_df = gtfs.stops(exclude_parent_stops=False)
        """execute_custom_query_pandas("SELECT stop_I, stop_id, code, name, desc, lat, lon, "
                                                          "CAST(parent_I AS INT) AS parent_I, "
                                                          "CAST(location_type AS INT) AS location_type,  "
                                                          "CAST(wheelchair_boarding AS INT) AS wheelchair_boarding, "
                                                          "self_or_parent_I"
                                                          " FROM stops")"""
        new_df["gtfs_id"] = i
        df = df.append(new_df)
        print("feed", i, "has", len(new_df.index), "stops")
    df = df.reset_index(drop=True)
    """
    min_stop_pair = df.copy()
    min_stop_pair["stop_pair_I"] = min_stop_pair.index
    min_stop_pair = min_stop_pair.groupby(by=["lat", "lon"]).agg({'stop_pair_I': "min"},  axis=1)
    min_stop_pair = min_stop_pair.reset_index()
    df = df.merge(min_stop_pair[["stop_pair_I", "lat", "lon"]], left_on=["lat", "lon"], right_on=["lat", "lon"])
    """
    df = _cluster_stops_multi(df, threshold_meters)

    # create a df of all stops without stop_pair_I duplicates
    df_all_default_stops = df.copy()
    df_all_default_parent_stops = df_all_default_stops.loc[~(df_all_default_stops.location_type == 0 |
                                                             df_all_default_stops.location_type.isnull())]

    df_all_default_stops = df_all_default_stops.loc[df_all_default_stops.location_type == 0 |
                                                    df_all_default_stops.location_type.isnull()]
    agg_dict = {i: lambda x: x.iloc[0] for i in list(df_all_default_stops)}
    df_all_default_stops = df_all_default_stops.groupby(by=['stop_pair_I']).agg(agg_dict,  axis=1)
    df_all_default_stops = df_all_default_stops.reset_index(drop=True)

    for i, gtfs in enumerate(gtfs_cons):
        i_upd_df = df.loc[df.gtfs_id == i].copy()
        # to_be_updated.append(i_upd_df)
        i_add_df = df_all_default_stops.loc[~(df_all_default_stops.stop_pair_I.isin(i_upd_df.stop_pair_I))].copy()
        #print(i_add_df)
        print("to add:", len(i_add_df.index), "rows")

        try:
            gtfs.execute_custom_query("""ALTER TABLE stops ADD COLUMN stop_pair_I INT""")
        except:
            pass
        query_update = "UPDATE stops SET stop_pair_I = ?, lat = ?, lon = ? WHERE stop_I = (?)"
        rows_to_update = [(int(a), float(b), float(c), int(d)) for a, b, c, d in
                          list(i_upd_df[['stop_pair_I', 'lat', 'lon', 'stop_I']].itertuples(index=False, name=None))]
        gtfs.conn.executemany(query_update, rows_to_update)
        gtfs.replace_stop_i_with_stop_pair_i(colname="stop_pair_I")

        rows_to_add = []

        for j, items in enumerate(i_add_df.itertuples(index=False)):
            try:
                rows_to_add.append((int(items.stop_pair_I),
                                    "__added__{i}_{j}__".format(i=i, j=j) + str(items.stop_id),
                                    str(items.code),
                                    str(items.name),
                                    str(items.desc),
                                    float(items.lat),
                                    float(items.lon),
                                    int(items.location_type) if isinstance(items.location_type, (int, float)) else items.location_type,
                                    bool(items.wheelchair_boarding)))
            except:
                print(items)
                raise

        query_add_row = """INSERT INTO stops(stop_I,
                                    stop_id,
                                    code,
                                    name,
                                    desc,
                                    lat,
                                    lon,
                                    location_type,
                                    wheelchair_boarding) VALUES (%s) """ % (", ".join(["?" for _ in range(9)]))

        gtfs.conn.executemany(query_add_row, rows_to_add)
        gtfs.conn.commit()


def aggregate_stops_spatially(gtfs, threshold_meters=2, order_by=None):
    """
    Aggregate stops spatially based on straight-line distances.
    Adjusts the underlying GTFS-database.

    Parameters
    ----------
    gtfs: gtfspy.GTFS
    threshold_meters: int, optional
    """
    # Get all stops that are not any other node's parent_stop_I
    non_parent_stops_df = gtfs.stops(exclude_parent_stops=True, order_by=order_by)

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
    # For each connected component, choose one representative stop_I,
    # and create a dictionary `to_new_stop_I' mapping original stop_I to the representative stop_I
    to_new_stop_I = dict()
    for component in components:
        component_iter = iter(sorted(component))
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


def df_to_utm_gdf(df):
    """
    Converts pandas dataframe with lon and lat columns to a geodataframe with a UTM projection
    :param df:
    :return:
    """
    df["geometry"] = df.apply(lambda row: Point((row["lon"], row["lat"])), axis=1)

    gdf = GeoDataFrame(df, crs=crs_wgs, geometry=df["geometry"])
    origin_centroid = MultiPoint(gdf["geometry"].tolist()).centroid
    srid = {'init': 'epsg:{srid}'.format(srid=get_utm_srid_from_wgs(origin_centroid.x, origin_centroid.y))}

    gdf = gdf.to_crs(crs=srid)
    return gdf, srid


def _cluster_stops_multi(df, distance):
    """

    :param df:
    :param distance: int, meters
    :return:
    """
    gdf, srid = df_to_utm_gdf(df)

    gdf_poly = gdf.copy()
    gdf_poly["geometry"] = gdf_poly["geometry"].buffer(distance)
    gdf_poly["everything"] = 1
    gdf_poly = gdf_poly.dissolve(by="everything")

    polygons = None
    for geoms in gdf_poly["geometry"]:
        polygons = [polygon for polygon in geoms]

    single_parts = GeoDataFrame(crs=srid, geometry=polygons)
    single_parts['stop_pair_I'] = single_parts.index
    gdf = sjoin(gdf, single_parts, how="left", op='within')
    single_parts["geometry"] = single_parts.centroid
    gdf = gdf.drop('geometry', 1)
    centroid_stops = single_parts.merge(gdf, on="stop_pair_I")

    centroid_stops = centroid_stops.to_crs(crs_wgs)
    centroid_stops["lat"] = centroid_stops.apply(lambda row: row.geometry.y, axis=1)
    centroid_stops["lon"] = centroid_stops.apply(lambda row: row.geometry.x, axis=1)
    centroid_stops = centroid_stops.drop('geometry', 1)
    return centroid_stops


def cluster_stops(gtfs_a, gtfs_b, distance=2):
    from shapely.geometry import Point
    from geopandas import GeoDataFrame, sjoin
    import pandas as pd
    import numpy as np

    crs_wgs = {'init': 'epsg:4326'}
    crs_eurefin = {'init': 'epsg:3067'}
    print("WARNING; THIS WORKS ONLY FOR AREAS WITHIN EUREFIN CRS")
    """
    merges stops that are within distance together into one stop
    :param stops_set: iterable that lists stop_I's
    :param distance: int, distance to merge, meters
    :return:
    """
    def get_stops_gdf(gtfs, feed_id):
        df = gtfs.execute_custom_query_pandas("SELECT stop_I, stop_id, code, name, desc, lat, lon, "
                                                          "CAST(parent_I AS INT) AS parent_I, "
                                                          "CAST(location_type AS INT) AS location_type,  "
                                                          "CAST(wheelchair_boarding AS INT) AS wheelchair_boarding, "
                                                          "self_or_parent_I "
                                                          " FROM stops")
        df["feed_id"] = feed_id

        df["geometry"] = df.apply(lambda row: Point((row["lon"], row["lat"])), axis=1)

        gdf = GeoDataFrame(df, crs=crs_wgs, geometry=df["geometry"])
        gdf = gdf.to_crs(crs_eurefin)

        return gdf

    gdf_a = get_stops_gdf(gtfs_a, "a")
    gdf_b = get_stops_gdf(gtfs_b, "b")
    gdf_ab = pd.concat([gdf_a, gdf_b])
    gdf_ab = gdf_ab.reset_index()
    gdf_poly = gdf_ab.copy()
    gdf_poly["geometry"] = gdf_poly["geometry"].buffer(distance/2)
    gdf_poly["everything"] = 1

    gdf_poly = gdf_poly.dissolve(by="everything")

    polygons = None
    for geoms in gdf_poly["geometry"]:
        polygons = [polygon for polygon in geoms]

    single_parts = GeoDataFrame(crs=crs_eurefin, geometry=polygons)
    single_parts['new_stop_I'] = single_parts.index
    gdf_joined = sjoin(gdf_ab, single_parts, how="left", op='within')
    single_parts["geometry"] = single_parts.centroid
    gdf_joined = gdf_joined.drop('geometry', 1)
    centroid_stops = single_parts.merge(gdf_joined, on="new_stop_I")
    """JOIN BY lat&lon"""
    # produce table with items to update and add to the two tables
    # add new id row to separete the tables
    # merge (outer) a & b dfs to this main df
    # Na rows-> rows to be added
    # other rows, rows to be updated
    def get_stops_to_add_and_update(gtfs, gdf, name):
        gdf = pd.merge(centroid_stops, gdf[['stop_I', 'feed_id', 'desc']], how='left', on=['stop_I', 'feed_id'],
                         suffixes=('', '_x'))

        nanrows = gdf.loc[gdf['desc_x'].isnull()]
        gdf = gdf.loc[gdf['desc_x'].notnull()]
        nanrows = nanrows.loc[~nanrows['new_stop_I'].isin(gdf['new_stop_I'])]
        stops_grouped = nanrows.groupby(['new_stop_I'])
        nanrows = stops_grouped.agg({'stop_id': lambda x: "__" + name + "__" + x.iloc[0],
                                     'code': lambda x: x.iloc[0],
                                     'name': lambda x: x.iloc[0],
                                     'desc': lambda x: x.iloc[0],
                                     'lat': lambda x: x.iloc[0],
                                     'lon': lambda x: x.iloc[0],
                                     'parent_I': lambda x: x.iloc[0],
                                     'location_type': lambda x: x.iloc[0],
                                     'wheelchair_boarding': lambda x: x.iloc[0],
                                     'self_or_parent_I': lambda x: x.iloc[0]}, axis=1)

        nanrows = nanrows.reset_index()
        try:
            gtfs.execute_custom_query("""ALTER TABLE stops ADD COLUMN stop_pair_I INT""")
        except:
            pass
        query_update = "UPDATE stops SET stop_pair_I = ? WHERE stop_I = (?)"
        rows_to_update = [(int(x), int(y)) for x, y in list(gdf[['new_stop_I', 'stop_I']].itertuples(index=False, name=None))]
        gtfs.conn.executemany(query_update, rows_to_update)

        query_add_row = """INSERT INTO stops(
                                    stop_pair_I,
                                    stop_id,
                                    code,
                                    name,
                                    desc,
                                    lat,
                                    lon,
                                    location_type,
                                    wheelchair_boarding) VALUES (%s) """ % (", ".join(["?" for _ in range(9)]))
        rows_to_add = []
        for items in nanrows.itertuples(index=False):
            rows_to_add.append((int(items.new_stop_I), str(items.stop_id),
                                        str(items.code),
                                         str(items.name),
                                         str(items.desc),
                                         float(items.lat),
                                         float(items.lon),
                                         int(items.location_type),
                                         bool(items.wheelchair_boarding)))

        gtfs.conn.executemany(query_add_row, rows_to_add)
        #list(nanrows[['new_stop_I', 'stop_id', 'code', 'name', 'desc',  'lat', 'lon', 'location_type', 'wheelchair_boarding']].itertuples(index=False, name=None)))
        gtfs.conn.commit()


    get_stops_to_add_and_update(gtfs_a, gdf_a, "a")
    get_stops_to_add_and_update(gtfs_b, gdf_b, "b")





    #print(gdf_a)