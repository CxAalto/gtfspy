from urllib.error import URLError

import numpy
import smopy
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import math
from gtfspy.gtfs import GTFS
from gtfspy.stats import get_spatial_bounds, get_percentile_stop_bounds, get_median_lat_lon_of_stops
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR, ROUTE_TYPE_TO_ZORDER, ROUTE_TYPE_TO_SHORT_DESCRIPTION
import matplotlib as mpl
from matplotlib_scalebar.scalebar import ScaleBar
from gtfspy import util

"""
This module contains functions for plotting (static) visualizations of the public transport networks using matplotlib.
"""
from gtfspy.extended_route_types import ROUTE_TYPE_CONVERSION

MAP_STYLES = [
    "rastertiles/voyager",
    "rastertiles/voyager_nolabels",
    "rastertiles/voyager_only_labels",
    "rastertiles/voyager_labels_under",
    "light_all",
    "dark_all",
    "light_nolabels",
    "light_only_labels",
    "dark_nolabels",
    "dark_only_labels"
]


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


def plot_route_network_from_gtfs(g, ax=None, spatial_bounds=None, map_alpha=0.8, scalebar=True, legend=True,
                                 return_smopy_map=False, map_style=None):
    """
    Parameters
    ----------
    g: A gtfspy.gtfs.GTFS object
        Where to get the data from?
    ax: matplotlib.Axes object, optional
        If None, a new figure and an axis is created
    spatial_bounds: dict, optional
        with str keys: lon_min, lon_max, lat_min, lat_max
    return_smopy_map: bool, optional
        defaulting to false

    Returns
    -------
    ax: matplotlib.axes.Axes

    """
    assert(isinstance(g, GTFS))
    route_shapes = g.get_all_route_shapes()

    if spatial_bounds is None:
        spatial_bounds = get_spatial_bounds(g, as_dict=True)
    if ax is not None:
        bbox = ax.get_window_extent().transformed(ax.figure.dpi_scale_trans.inverted())
        width, height = bbox.width, bbox.height
        spatial_bounds = _expand_spatial_bounds_to_fit_axes(spatial_bounds, width, height)
    return plot_as_routes(route_shapes,
                          ax=ax,
                          spatial_bounds=spatial_bounds,
                          map_alpha=map_alpha,
                          plot_scalebar=scalebar,
                          legend=legend,
                          return_smopy_map=return_smopy_map,
                          map_style=map_style)


def plot_as_routes(route_shapes, ax=None, spatial_bounds=None, map_alpha=0.8, plot_scalebar=True, legend=True,
                   return_smopy_map=False, line_width_attribute=None, line_width_scale=1.0, map_style=None):
    """
    Parameters
    ----------
    route_shapes: list of dicts that should have the following keys
            name, type, agency, lats, lons
            with types
            list, list, str, list, list
    ax: axis object
    spatial_bounds: dict
    map_alpha:
    plot_scalebar: bool
    legend:
    return_smopy_map:
    line_width_attribute:
    line_width_scale:

    Returns
    -------
    ax: matplotlib.axes object
    """
    lon_min = spatial_bounds['lon_min']
    lon_max = spatial_bounds['lon_max']
    lat_min = spatial_bounds['lat_min']
    lat_max = spatial_bounds['lat_max']
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)

    smopy_map = get_smopy_map(lon_min, lon_max, lat_min, lat_max, map_style=map_style)
    ax = smopy_map.show_mpl(figsize=None, ax=ax, alpha=map_alpha)
    bound_pixel_xs, bound_pixel_ys = smopy_map.to_pixels(numpy.array([lat_min, lat_max]),
                                                         numpy.array([lon_min, lon_max]))

    route_types_to_lines = {}
    for shape in route_shapes:
        route_type = ROUTE_TYPE_CONVERSION[shape['type']]
        lats = numpy.array(shape['lats'])
        lons = numpy.array(shape['lons'])
        if line_width_attribute:
            line_width = line_width_scale * shape[line_width_attribute]
        else:
            line_width = 1
        xs, ys = smopy_map.to_pixels(lats, lons)
        line, = ax.plot(xs, ys, linewidth=line_width, color=ROUTE_TYPE_TO_COLOR[route_type], zorder=ROUTE_TYPE_TO_ZORDER[route_type])
        route_types_to_lines[route_type] = line

    if legend:
        lines = list(route_types_to_lines.values())
        labels = [ROUTE_TYPE_TO_SHORT_DESCRIPTION[route_type] for route_type in route_types_to_lines.keys()]
        ax.legend(lines, labels, loc="upper left")

    if plot_scalebar:
        _add_scale_bar(ax, lat_max, lon_min, lon_max, bound_pixel_xs.max() - bound_pixel_xs.min())

    ax.set_xticks([])
    ax.set_yticks([])

    ax.set_xlim(bound_pixel_xs.min(), bound_pixel_xs.max())
    ax.set_ylim(bound_pixel_ys.max(), bound_pixel_ys.min())
    if return_smopy_map:
        return ax, smopy_map
    else:
        return ax


def plot_routes_as_stop_to_stop_network(from_lats, from_lons, to_lats, to_lons, attributes=None, color_attributes=None,
                                        zorders=None,
                                        line_labels=None,
                                        ax=None,
                                        spatial_bounds=None,
                                        alpha=1,
                                        map_alpha=0.8,
                                        scalebar=True,
                                        return_smopy_map=False,
                                        c=None, linewidth=None,
                                        linewidth_multiplier=1,
                                        use_log_scale=False):
    if attributes is None:
        attributes = len(list(from_lats))*[None]
        if not linewidth:
            linewidth = 1
    if color_attributes is None:
        color_attributes = len(list(from_lats))*[None]
        assert c is not None
    if zorders is None:
        zorders = len(list(from_lats))*[1]
    if line_labels is None:
        line_labels = len(list(from_lats))*[None]

    if spatial_bounds is None:
        lon_min = min(list(from_lons) + list(to_lons))
        lon_max = max(list(from_lons) + list(to_lons))
        lat_min = min(list(from_lats) + list(to_lats))
        lat_max = max(list(from_lats) + list(to_lats))
    else:
        lon_min = spatial_bounds['lon_min']
        lon_max = spatial_bounds['lon_max']
        lat_min = spatial_bounds['lat_min']
        lat_max = spatial_bounds['lat_max']
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)

    smopy_map = get_smopy_map(lon_min, lon_max, lat_min, lat_max)
    ax = smopy_map.show_mpl(figsize=None, ax=ax, alpha=map_alpha)
    bound_pixel_xs, bound_pixel_ys = smopy_map.to_pixels(numpy.array([lat_min, lat_max]),
                                                         numpy.array([lon_min, lon_max]))

    for from_lat, from_lon, to_lat, to_lon, attribute, color_attribute, zorder, line_label in zip(from_lats,
                                                                                                  from_lons,
                                                                                                  to_lats,
                                                                                                  to_lons,
                                                                                                  attributes,
                                                                                                  color_attributes,
                                                                                                  zorders,
                                                                                                  line_labels):



        if color_attribute is None:
            color = c
        else:
            a = ROUTE_TYPE_CONVERSION[color_attribute]
            color = ROUTE_TYPE_TO_COLOR[a]
            zorder = ROUTE_TYPE_TO_ZORDER[a]
        if not attribute:
            attribute = linewidth
        if use_log_scale:
            attribute = math.log10(attribute)

        xs, ys = smopy_map.to_pixels(numpy.array([from_lat, to_lat]), numpy.array([from_lon, to_lon]))

        ax.plot(xs, ys, color=color, linewidth=attribute*linewidth_multiplier, zorder=zorder, alpha=alpha)
        if line_label:
            ax.text(xs.mean(), ys.mean(), line_label,
                    # verticalalignment='bottom', horizontalalignment='right',
                    color='green', fontsize=15)

    legend = True if color_attributes[0] is not None else False
    import matplotlib.lines as mlines
        
    if legend:
        unique_types = set(color_attributes)
        lines = []
        
        for i in unique_types:
            line = mlines.Line2D([], [], color=ROUTE_TYPE_TO_COLOR[i], markersize=15,
                                 label=ROUTE_TYPE_TO_SHORT_DESCRIPTION[i])

            lines.append(line)
        handles = lines
        labels = [h.get_label() for h in handles] 
            
        ax.legend(handles=handles, labels=labels, loc=4)
        
    if scalebar:
        _add_scale_bar(ax, lat_max, lon_min, lon_max, bound_pixel_xs.max() - bound_pixel_xs.min())

    ax.set_xticks([])
    ax.set_yticks([])

    ax.set_xlim(bound_pixel_xs.min(), bound_pixel_xs.max())
    ax.set_ylim(bound_pixel_ys.max(), bound_pixel_ys.min())

    if return_smopy_map:
        return ax, smopy_map
    else:
        return ax


def _add_scale_bar(ax, lat, lon_min, lon_max, width_pixels):
    distance_m = util.wgs84_distance(lat, lon_min, lat, lon_max)
    scalebar = ScaleBar(distance_m / width_pixels)  # 1 pixel = 0.2 meter
    ax.add_artist(scalebar)


def _expand_spatial_bounds_to_fit_axes(bounds, ax_width, ax_height):
    """
    Parameters
    ----------
    bounds: dict
    ax_width: float
    ax_height: float

    Returns
    -------
    spatial_bounds
    """
    b = bounds
    height_meters = util.wgs84_distance(b['lat_min'], b['lon_min'], b['lat_max'], b['lon_min'])
    width_meters = util.wgs84_distance(b['lat_min'], b['lon_min'], b['lat_min'], b['lon_max'])
    x_per_y_meters = width_meters / height_meters
    x_per_y_axes = ax_width / ax_height
    if x_per_y_axes > x_per_y_meters:  # x-axis
        # axis x_axis has slack -> the spatial longitude bounds need to be extended
        width_meters_new = (height_meters * x_per_y_axes)
        d_lon_new = ((b['lon_max'] - b['lon_min']) / width_meters) * width_meters_new
        mean_lon = (b['lon_min'] + b['lon_max'])/2.
        lon_min = mean_lon - d_lon_new / 2.
        lon_max = mean_lon + d_lon_new / 2.
        spatial_bounds = {
            "lon_min": lon_min,
            "lon_max": lon_max,
            "lat_min": b['lat_min'],
            "lat_max": b['lat_max']
        }
    else:
        # axis y_axis has slack -> the spatial latitude bounds need to be extended
        height_meters_new = (width_meters / x_per_y_axes)
        d_lat_new = ((b['lat_max'] - b['lat_min']) / height_meters) * height_meters_new
        mean_lat = (b['lat_min'] + b['lat_max']) / 2.
        lat_min = mean_lat - d_lat_new / 2.
        lat_max = mean_lat + d_lat_new / 2.
        spatial_bounds = {
            "lon_min": b['lon_min'],
            "lon_max": b['lon_max'],
            "lat_min": lat_min,
            "lat_max": lat_max
        }
    return spatial_bounds



def plot_route_network_thumbnail(g, map_style=None):
    width = 512  # pixels
    height = 300  # pixels
    scale = 24
    dpi = mpl.rcParams["figure.dpi"]

    width_m = width * scale
    height_m = height * scale
    median_lat, median_lon = get_median_lat_lon_of_stops(g)
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
    return plot_route_network_from_gtfs(g, ax, spatial_bounds, map_alpha=1.0, scalebar=False, legend=False, map_style=map_style)


def plot_stops_with_categorical_attributes(lats_list, lons_list, attributes_list, s=0.5, spatial_bounds=None,  colorbar=False, ax=None, cmap=None, norm=None, alpha=None):
    if not spatial_bounds:
        lon_min = min([min(x) for x in lons_list])
        lon_max = max([max(x) for x in lons_list])
        lat_min = min([min(x) for x in lats_list])
        lat_max = max([max(x) for x in lats_list])
    else:
        lon_min = spatial_bounds['lon_min']
        lon_max = spatial_bounds['lon_max']
        lat_min = spatial_bounds['lat_min']
        lat_max = spatial_bounds['lat_max']
    smopy_map = get_smopy_map(lon_min, lon_max, lat_min, lat_max)
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    min_x = max_x = min_y = max_y = None
    for lat in [lat_min, lat_max]:
        for lon in [lon_min, lon_max]:
            x, y = smopy_map.to_pixels(lat, lon)
            if not min_x:
                min_x = x
                max_x = x
                min_y = y
                max_y = y
            else:
                max_x = max(max_x, x)
                max_y = max(max_y, y)
                min_y = min(min_y, y)
                min_x = min(min_x, x)

    ax.set_xlim(min_x, max_x)
    ax.set_ylim(max_y, min_y)
    ax.set_xticks([])
    ax.set_yticks([])
    ax = smopy_map.show_mpl(figsize=None, ax=ax, alpha=0.8)

    axes = []
    for lats, lons, attributes, c in zip(lats_list, lons_list, attributes_list, mcolors.BASE_COLORS):
        x, y = zip(*[smopy_map.to_pixels(lat, lon) for lat, lon in zip(lats, lons)])
        ax = plt.scatter(x, y, s=s, c=c) #, marker=".")
        axes.append(ax)
    return axes


def plot_stops_with_attributes(lats, lons, attribute, s=0.5, spatial_bounds=None, colorbar=False, ax=None, cmap=None, norm=None, alpha=None):
    if not spatial_bounds:
        lon_min = min(lons)
        lon_max = max(lons)
        lat_min = min(lats)
        lat_max = max(lats)
    else:
        lon_min = spatial_bounds['lon_min']
        lon_max = spatial_bounds['lon_max']
        lat_min = spatial_bounds['lat_min']
        lat_max = spatial_bounds['lat_max']
    smopy_map = get_smopy_map(lon_min, lon_max, lat_min, lat_max)
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    ax = smopy_map.show_mpl(figsize=None, ax=ax, alpha=0.8)

    xs, ys = smopy_map.to_pixels(lats, lons)
    cax = ax.scatter(xs, ys, c=attribute, s=s, cmap=cmap, norm=norm, alpha=alpha)

    ax.set_xlim(min(xs), max(xs))
    ax.set_ylim(max(ys), min(ys))
    if colorbar:
        return ax, cax, smopy_map
    return ax


def plot_all_stops(g, ax=None, scalebar=False):
    """
    Parameters
    ----------
    g: A gtfspy.gtfs.GTFS object
    ax: matplotlib.Axes object, optional
        If None, a new figure and an axis is created, otherwise results are plotted on the axis.
    scalebar: bool, optional
        Whether to include a scalebar to the plot.

    Returns
    -------
    ax: matplotlib.Axes

    """
    assert(isinstance(g, GTFS))
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


def get_smopy_map(lon_min, lon_max, lat_min, lat_max, z=None, map_style=None):

    if map_style is not None:
        assert map_style in MAP_STYLES, map_style + \
                                        " (map_style parameter) is not a valid CartoDB mapping style. Options are " + \
                                        str(MAP_STYLES)
        tileserver = "http://1.basemaps.cartocdn.com/" + map_style + "/{z}/{x}/{y}.png"
    else:
        tileserver = "http://1.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"

    args = (lat_min, lat_max, lon_min, lon_max, map_style, z)
    if args not in get_smopy_map.maps:
        kwargs = {}
        if z is not None:  # this hack may not work
            smopy.Map.get_allowed_zoom = lambda self, z: z
            kwargs['z'] = z
        try:
            get_smopy_map.maps[args] = smopy.Map((lat_min, lon_min, lat_max, lon_max), tileserver=tileserver, **kwargs)
        except URLError:
            raise RuntimeError("\n Could not load background map from the tile server: "
                               + tileserver +
                               "\n Please check that the tile server exists and "
                               "that your are connected to the internet.")

    return get_smopy_map.maps[args]


get_smopy_map.maps = {}
