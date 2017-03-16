from urllib.error import URLError

import numpy
import smopy
import matplotlib.pyplot as plt
from gtfspy.gtfs import GTFS
from gtfspy.stats import get_spatial_bounds
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR

"""
This module contains functions for plotting (static) visualizations of the public transport networks using matplotlib.
"""

smopy.TILE_SERVER = "https://cartodb-basemaps-1.global.ssl.fastly.net/dark_all/{z}/{x}/{y}.png"


def plot_route_network(g, ax=None):
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
    stats = g.get_stats()
    for key, val in stats.items():
        print(key, val)
    lon_min, lon_max, lat_min, lat_max = get_spatial_bounds(g)
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    route_shapes = g.get_all_route_shapes()
    print(lat_min, lat_max)
    print(lon_min, lon_max)
    smopy_map = get_smopy_map(lon_min, lon_max, lat_min, lat_max)
    ax = smopy_map.show_mpl(figsize=None, ax=ax, alpha=0.8)
    pixel_x_max = -float('inf')
    pixel_x_min = float('inf')
    pixel_y_max = -float('inf')
    pixel_y_min = float('inf')
    for shape in route_shapes:
        mode = shape['type']
        lats = numpy.array(shape['lats'])
        lons = numpy.array(shape['lons'])
        xs, ys = smopy_map.to_pixels(lats, lons)
        ax.plot(xs, ys, color=ROUTE_TYPE_TO_COLOR[mode])

        # update pixel bounds
        pixel_x_min = min(xs.min(), pixel_x_min)
        pixel_x_max = max(xs.max(), pixel_x_max)
        pixel_y_min = min(ys.min(), pixel_y_min)
        pixel_y_max = max(ys.max(), pixel_y_max)
    ax.set_xlim(pixel_x_min, pixel_x_max)
    ax.set_ylim(pixel_y_max, pixel_y_min)
    return ax


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
    return get_smopy_map.maps[args]

get_smopy_map.maps = {}
