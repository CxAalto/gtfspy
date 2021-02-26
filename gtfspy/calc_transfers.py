from __future__ import print_function

import math
import operator

from geoindex import GeoGridIndex, GeoPoint
from geoindex.geo_grid_index import GEO_HASH_GRID_SIZE

from gtfspy.gtfs import GTFS
from gtfspy.util import wgs84_distance, wgs84_height, wgs84_width

create_stmt = ('CREATE TABLE IF NOT EXISTS main.stop_distances '
               '(from_stop_I INT, '
               ' to_stop_I INT, '
               ' d INT, '
               ' d_walk INT, '
               ' min_transfer_time INT, '
               ' timed_transfer INT, '
               'UNIQUE (from_stop_I, to_stop_I)'
               ')'
               )


def bind_functions(conn):
    conn.create_function("find_distance", 4, wgs84_distance)
    conn.create_function("wgs84_height", 1, wgs84_height)
    conn.create_function("wgs84_width", 2, wgs84_width)


def _get_geo_hash_precision(search_radius_in_km):
    # adapted from geoindex.geo_grid_index
    suggested_precision = None
    for precision, max_size in sorted(GEO_HASH_GRID_SIZE.items(), key=operator.itemgetter(1)):
        # ordering is from smallest grid size to largest:
        if search_radius_in_km < max_size / 2:
            suggested_precision = precision
            break
    if suggested_precision is None:
        raise RuntimeError("GeoHash cannot work with this large search radius (km): " + search_radius_in_km)
    return suggested_precision


def calc_transfers(conn, threshold_meters=1000):
    g = GTFS(conn)
    cursor = conn.cursor()
    geohash_precision = _get_geo_hash_precision(threshold_meters / 1000.)
    geo_index = GeoGridIndex(precision=geohash_precision)
    stops = g.get_table("stops")
    stop_geopoints = []

    for stop in stops.itertuples():
        stop_geopoint = GeoPoint(stop.lat, stop.lon, ref=stop.stop_I)
        geo_index.add_point(stop_geopoint)
        stop_geopoints.append(stop_geopoint)
    for stop_geopoint in stop_geopoints:
        nearby_stop_geopoints = geo_index.get_nearest_points_dirty(stop_geopoint, threshold_meters / 1000.0, "km")
        from_stop_I = int(stop_geopoint.ref)
        from_lat = stop_geopoint.latitude
        from_lon = stop_geopoint.longitude

        to_stop_Is = []
        distances = []
        for nearby_stop_geopoint in nearby_stop_geopoints:
            to_stop_I = int(nearby_stop_geopoint.ref)
            if to_stop_I == from_stop_I:
                continue
            to_lat = nearby_stop_geopoint.latitude
            to_lon = nearby_stop_geopoint.longitude
            distance = math.ceil(wgs84_distance(from_lat, from_lon, to_lat, to_lon))
            if distance <= threshold_meters:
                to_stop_Is.append(to_stop_I)
                distances.append(distance)

        n_pairs = len(to_stop_Is)
        from_stop_Is = [from_stop_I] * n_pairs
        cursor.executemany('INSERT OR REPLACE INTO stop_distances VALUES (?, ?, ?, ?, ?, ?);',
                           zip(from_stop_Is, to_stop_Is, distances, [None] * n_pairs,
                               [None] * n_pairs, [None] * n_pairs))
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sd_fsid ON stop_distances (from_stop_I);')
        conn.commit()


def calc_transfers_using_geopandas(conn, threshold_meters=1000, return_sd_table=False, sd_table=None):
    """
    This ended up as slower/as fast compared to the original calc_transfers function.
    Performance depends on how the points are distributed
    :return:
    """
    from geopandas import GeoDataFrame, sjoin
    from gtfspy.util import get_utm_srid_from_wgs
    from shapely.geometry import Point, MultiPoint

    g = GTFS(conn)
    cursor = conn.cursor()
    gdf_joined = sd_table
    if gdf_joined is None:
        stops = g.get_table("stops")
        crs_wgs = {'init': 'epsg:4326'}

        stops["geometry"] = stops.apply(lambda row: Point((row["lon"], row["lat"])), axis=1)

        gdf = GeoDataFrame(stops, crs=crs_wgs, geometry=stops["geometry"])
        origin_centroid = MultiPoint(gdf["geometry"].tolist()).centroid
        srid = get_utm_srid_from_wgs(origin_centroid.x, origin_centroid.y)
        gdf = gdf.to_crs({'init': 'epsg:{srid}'.format(srid=srid)})

        gdf["from_stop_I"] = gdf["stop_I"]
        gdf["point_a"] = gdf["geometry"]
        gdf = gdf[["from_stop_I", "point_a", "geometry"]]
        gdf_poly = gdf.copy()
        gdf_poly["point_b"] = gdf_poly["point_a"]
        gdf_poly["to_stop_I"] = gdf_poly["from_stop_I"]
        gdf_poly = gdf_poly[["to_stop_I", "point_b", "geometry"]]

        gdf_poly["geometry"] = gdf_poly["geometry"].buffer_distance(threshold_meters)
        gdf_joined = sjoin(gdf, gdf_poly, how="left", op='within')
        gdf_joined["d"] = gdf_joined.apply(lambda row: int(row.point_a.distance(row.point_b)), axis=1)
        gdf_joined = gdf_joined.loc[gdf_joined.from_stop_I != gdf_joined.to_stop_I]

    n_pairs = len(gdf_joined.index)

    cursor.executemany('INSERT OR REPLACE INTO stop_distances VALUES (?, ?, ?, ?, ?, ?);',
                       zip(gdf_joined["from_stop_I"].tolist(), gdf_joined["to_stop_I"].tolist(),
                           gdf_joined["d"].tolist(), [None] * n_pairs,
                           [None] * n_pairs, [None] * n_pairs))
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sd_fsid ON stop_distances (from_stop_I);')
    conn.commit()
    if return_sd_table:
        return gdf_joined


def _export_transfers(conn, fname):
    conn = GTFS(conn).conn
    cur = conn.cursor()
    cur.execute('SELECT S1.lat, S1.lon, S2.lat, S2.lon, SD.d '
                'FROM stop_distances SD '
                '  LEFT JOIN stops S1 ON (SD.from_stop_I=S1.stop_I) '
                '  LEFT JOIN stops S2 ON (SD.to_stop_I  =S2.stop_I)')
    f = open(fname, 'w')
    for row in cur:
        print(' '.join(str(x) for x in row), file=f)


def main():
    import sys
    cmd = sys.argv[1]
    if cmd == 'calc':
        dbname = sys.argv[2]
        conn = GTFS(dbname).conn
        calc_transfers(conn)
    elif cmd == 'export':
        _export_transfers(sys.argv[2], sys.argv[3])


if __name__ == "__main__":
    main()
