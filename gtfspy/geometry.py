from shapely.geometry import MultiPoint
from pyproj import Proj

def get_convex_hull_coordinates(gtfs):
    """

    Parameters
    ----------
    gtfs: gtfs.GTFS

    Returns
    -------
    lons: list
        of floats
    lats: list
        of floats
    """
    lons, lats = _get_stop_lat_lons(gtfs)
    lon_lats = list(zip(lons, lats))
    polygon = MultiPoint(lon_lats).convex_hull
    hull_lons, hull_lats= polygon.exterior.coords.xy
    return hull_lats, hull_lons

def _get_stop_lat_lons(gtfs):
    stops = gtfs.stops()
    lats = stops['lat']
    lons = stops['lon']
    return lons, lats

def get_approximate_convex_hull_area_km2(gtfs):
    lons, lats = _get_stop_lat_lons(gtfs)
    return approximate_convex_hull_area(lons, lats)

def approximate_convex_hull_area(lons, lats):
    lat_min = min(lats)
    lat_max = max(lats)
    lat_mean = (lat_max + lat_min) / 2.
    lon_min = min(lons)
    lon_max = max(lons)
    lon_mean = (lon_max + lon_min) / 2.

    from gtfspy.util import wgs84_distance
    lat_span_meters = wgs84_distance(lat_min, lon_mean, lat_max, lon_mean)
    lon_span_meters = wgs84_distance(lat_mean, lon_min, lat_mean, lon_max)

    lat_meters = [(lat - lat_min) / (lat_max - lat_min) * lat_span_meters for lat in lats]
    lon_meters = [(lon - lon_min) / (lon_max - lon_min) * lon_span_meters for lon in lons]
    lon_lat_meters = list(zip(lon_meters, lat_meters))
    return MultiPoint(lon_lat_meters).convex_hull.area / 1000 ** 2


