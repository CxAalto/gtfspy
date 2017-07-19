from urllib.error import URLError

import numpy
import smopy
import matplotlib.pyplot as plt
from gtfspy.gtfs import GTFS
from gtfspy.stats import get_spatial_bounds, get_percentile_stop_bounds, get_median_lat_lon_of_stops
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR, ROUTE_TYPE_TO_ZORDER
import matplotlib as mpl

"""
This module contains functions for plotting (static) visualizations of the public transport networks using matplotlib.
"""

def _get_median_centered_plot_bounds(g):
    lon_min, lon_max, lat_min, lat_max = get_spatial_bounds(g)
    lat_median, lon_median = get_median_lat_lon_of_stops(g)
    lon_diff = max(abs(lon_median - lon_min), abs(lon_median - lon_max))
    lat_diff = max(abs(lat_median - lat_min), abs(lat_median - lat_max))
    plot_lon_min = lon_median - lon_diff
    plot_lon_max = lon_median + lon_diff
    plot_lat_min = lat_median - lat_diff
    plot_lat_max = lat_median + lat_diff
    return plot_lon_min, plot_lon_max, plot_lat_min, plot_lat_max


def plot_route_network(g, ax=None, spatial_bounds=None, map_alpha=0.8):
    """
    Parameters
    ----------
    g: A gtfspy.gtfs.GTFS object
        Where to get the data from?
    ax: matplotlib.Axes object, optional
        If None, a new figure and an axis is created

    Returns
    -------
    ax: matplotlib

    """
    assert(isinstance(g, GTFS))
    if spatial_bounds is None:
        lon_min, lon_max, lat_min, lat_max = get_spatial_bounds(g)
    else:
        lon_min = spatial_bounds['lon_min']
        lon_max = spatial_bounds['lon_max']
        lat_min = spatial_bounds['lat_min']
        lat_max = spatial_bounds['lat_max']
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    route_shapes = g.get_all_route_shapes()
    print(lat_min, lat_max)
    print(lon_min, lon_max)
    smopy_map = get_smopy_map(lon_min, lon_max, lat_min, lat_max)
    ax = smopy_map.show_mpl(figsize=None, ax=ax, alpha=map_alpha)
    bound_pixel_xs, bound_pixel_ys = smopy_map.to_pixels(numpy.array([lat_min, lat_max]),
                                                         numpy.array([lon_min, lon_max]))


    for shape in route_shapes:
        route_type = shape['type']
        lats = numpy.array(shape['lats'])
        lons = numpy.array(shape['lons'])
        xs, ys = smopy_map.to_pixels(lats, lons)
        ax.plot(xs, ys, color=ROUTE_TYPE_TO_COLOR[route_type], zorder=ROUTE_TYPE_TO_ZORDER[route_type])
        # update pixel bounds

    ax.set_xlim(bound_pixel_xs.min(), bound_pixel_xs.max())
    ax.set_ylim(bound_pixel_ys.max(), bound_pixel_ys.min())
    return ax


def plot_route_network_thumbnail(g):
    width = 512  # pixels
    height = 300  # pixels
    scale = 24
    dpi = mpl.rcParams["figure.dpi"]

    width_m = width * scale
    height_m = height * scale
    median_lat, median_lon = get_median_lat_lon_of_stops(g)
    from gtfspy import util
    dlat = util.wgs84_height(height_m)
    dlon = util.wgs84_width(width_m, median_lat)
    spatial_bounds = {
        "lon_min": median_lon - dlon,
        "lon_max": median_lon + dlon,
        "lat_min": median_lat - dlat,
        "lat_max": median_lat + dlat
    }
    fig = plt.figure(figsize=(width/dpi, height/dpi))
    ax = fig.add_subplot(111)
    plt.subplots_adjust(bottom=0.0, left=0.0, right=1.0, top=1.0)
    return plot_route_network(g, ax, spatial_bounds, map_alpha=1.0)


def plot_all_stops(g, ax=None):
    """
    Parameters
    ----------
    g: A gtfspy.gtfs.GTFS object
    ax: matplotlib.Axes object, optional
        If None, a new figure and an axis is created, otherwise results are plotted on the axis.

    Returns
    -------
    ax: matplotlib

    """
    assert(isinstance(g, GTFS))
    stats = g.get_stats()
    for key, val in stats.items():
        print(key, val)
    lon_min, lon_max, lat_min, lat_max = get_spatial_bounds(g)
    smopy_map = get_smopy_map(lon_min, lon_max, lat_min, lat_max)
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    ax = smopy_map.show_mpl(figsize=None, ax=ax, alpha=0.8)

    stops = g.stops()
    lats = numpy.array(stops['lat'])
    lons = numpy.array(stops['lon'])

    xs, ys = smopy_map.to_pixels(lats, lons)
    ax.scatter(xs, ys, color="red", s=10)

    ax.set_xlim(min(xs), max(xs))
    ax.set_ylim(max(ys), min(ys))
    return ax


def get_smopy_map(lon_min, lon_max, lat_min, lat_max, z=None):
    OLD_SMOPY_TILE_SERVER = smopy.TILE_SERVER
    smopy.TILE_SERVER = "https://cartodb-basemaps-1.global.ssl.fastly.net/dark_all/{z}/{x}/{y}.png"
    args = (lat_min, lat_max, lon_min, lon_max, z)
    if args not in get_smopy_map.maps:
        kwargs = {}
        if z is not None:  # this hack may not work
            smopy.Map.get_allowed_zoom = lambda self, z: z
            kwargs['z'] = z
        try:
            get_smopy_map.maps[args] = smopy.Map((lat_min, lon_min, lat_max, lon_max), **kwargs)
        except URLError:
            raise RuntimeError("\n Could not load background map from the tile server: " + smopy.TILE_SERVER +
                               "\n Please check that the tile server exists and "
                               "that your are connected to the internet.")
    smopy.TILE_SERVER = OLD_SMOPY_TILE_SERVER
    return get_smopy_map.maps[args]

get_smopy_map.maps = {}
