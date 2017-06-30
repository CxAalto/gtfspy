from shapely.geometry import MultiPoint


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
    lon_meters, lat_meters = _get_lon_lat_meters(lons, lats)
    lon_lat_meters = list(zip(lon_meters, lat_meters))
    return MultiPoint(lon_lat_meters).convex_hull.area / 1000 ** 2

def _get_lon_lat_meters(lons, lats):
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
    return lon_meters, lat_meters


def get_buffered_area_of_stops(gtfs, buffer_meters, resolution):
    """

    Compute the total area of all buffered stops in PT network.

    Parameters
    ----------
    gtfs: gtfs.GTFS
    buffer_meters: meters around the stop to buffer.
    resolution: increases the accuracy of the calculated area with computation time. Default = 16 

    Returns
    -------
    Total area covered by the buffered stops in square meters.
    """
    lons, lats = _get_stop_lat_lons(gtfs)
    a = compute_buffered_area_of_stops(lats, lons, buffer_meters, resolution)
    return a


def compute_buffered_area_of_stops(lats, lons, buffer_meters, resolution=16):
    # geo_series = gp.GeoSeries([Point(lon, lat) for lon, lat in zip(lons, lats)])
    # geo_series.crs = {'init' :'epsg:4326'}
    # geo_series = geo_series.to_crs({'init':'epsg:3857'})

    # circles = geo_series.buffer(buffer_meters, resolution=resolution)
    # multi_points = circles.unary_union
    # return multi_points.area

    if len(lons) > 1:
        lon_meters, lat_meters = _get_lon_lat_meters(lons, lats)
    else:
        lon_meters = lons
        lat_meters = lats

    return MultiPoint(points=list(zip(lon_meters, lat_meters))).buffer(buffer_meters, resolution=resolution).area

