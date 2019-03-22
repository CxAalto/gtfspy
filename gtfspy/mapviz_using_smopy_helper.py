import math
from urllib.error import URLError

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from mpl_toolkits import axes_grid1
import numpy
import smopy
from matplotlib import colors as mcolors
from matplotlib_scalebar.scalebar import ScaleBar

import gtfspy.smopy_plot_helper
from gtfspy import util
from gtfspy.gtfs import GTFS
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR, ROUTE_TYPE_TO_ZORDER, ROUTE_TYPE_TO_SHORT_DESCRIPTION
from gtfspy.stats import get_spatial_bounds, get_median_lat_lon_of_stops

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


def plot_stops_with_categorical_attributes(lats_list, lons_list, attributes_list, labels=None, s=1,
                                           spatial_bounds=None,
                                           colors=None,
                                           markers=None, scalebar=True):
    if not colors:
        colors = mcolors.BASE_COLORS
    if not markers:
        markers = ["."]*5

    fig = plt.figure()
    ax = fig.add_subplot(111, projection="smopy_axes")

    axes = []
    for lats, lons, attributes, c, marker, label in zip(lats_list, lons_list, attributes_list, colors, markers, labels):
        ax.scatter(lons, lats, s=s, c=c, marker=marker, label=label)

    if scalebar:
        ax.add_scalebar(**{"frameon": False, "location": "lower right"})
    if spatial_bounds:
        ax.set_plot_bounds(**spatial_bounds)
    ax.set_xticks([])
    ax.set_yticks([])

    return ax


def plot_stops_with_attributes_smopy(lats, lons, attributes, s=1, alpha=1,
                                     ax=None,
                                     spatial_bounds=None,
                                     cmap=None,
                                     norm=None,
                                     marker=None, scalebar=True):
    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111, projection="smopy_axes")

    cax = ax.scatter(lons, lats, alpha=alpha, c=attributes, s=s, cmap=cmap, norm=norm, marker=marker)

    if scalebar:
        ax.add_scalebar(frameon=False, location="lower right")
    if spatial_bounds:
        ax.set_plot_bounds(**spatial_bounds)
    ax.set_xticks([])
    ax.set_yticks([])
    return ax, cax


def plot_routes_as_stop_to_stop_network(from_lats, from_lons, to_lats, to_lons, attributes=None, color_attributes=None,
                                        zorders=None,
                                        line_labels=None,
                                        ax=None,
                                        spatial_bounds=None,
                                        alpha=1,
                                        map_alpha=0.8,
                                        scalebar=True,
                                        c=None, linewidth=None,
                                        linewidth_multiplier=1,
                                        use_log_scale=False,
                                        legend_multiplier=1,
                                        legend_unit=""):
    if not linewidth:
        linewidth = 1
    if attributes is None:
        attributes = len(list(from_lats)) * [linewidth]
    if use_log_scale:
        attributes = [math.log10(x) for x in attributes]
    else:
        attributes = [x*linewidth_multiplier for x in attributes]

    if color_attributes is None:
        assert c is not None
        colors = len(list(from_lats)) * [c]

    else:
        route_types = [ROUTE_TYPE_CONVERSION[x] for x in color_attributes]
        colors = [ROUTE_TYPE_TO_COLOR[x] for x in route_types]
        zorders = [ROUTE_TYPE_TO_ZORDER[x] for x in route_types]

    if zorders is None:
        zorders = len(list(from_lats)) * [1]
    if line_labels is None:
        line_labels = len(list(from_lats)) * [None]

    if ax is None:
        fig = plt.figure()
        ax = fig.add_subplot(111, projection="smopy_axes")

    coords = [[(from_lon, from_lat), (to_lon, to_lat)] for from_lon, from_lat, to_lon, to_lat in zip(from_lons,
                                                                                                     from_lats,
                                                                                                     to_lons,
                                                                                                     to_lats)]

    ax.plot_line_segments(coords, attributes, colors, zorders)

    legend = True if color_attributes[0] is not None else False

    if legend:
        unique_types = set(color_attributes)
        lines = []

        for i in unique_types:
            line = mlines.Line2D([], [], color=ROUTE_TYPE_TO_COLOR[i], markersize=15,
                                 label=ROUTE_TYPE_TO_SHORT_DESCRIPTION[i])

            lines.append(line)

        for i in [50, 100, 200, 500, 1000]:
            line = mlines.Line2D([], [], color="black", linewidth=i*linewidth_multiplier*legend_multiplier,
                                 label="{0: >4}".format(str(i*legend_multiplier))
                                 if not i == 200 else "{0: >4}".format(str(i*legend_multiplier)) + " " + legend_unit,
                                 solid_capstyle='butt')

            lines.append(line)
        handles = lines
        labels = [h.get_label() for h in handles]

        ax.legend(handles=handles, labels=labels, loc=2, ncol=2, prop={'size': 7})

    if scalebar:
        ax.add_scalebar(frameon=False, location="lower right")
    if spatial_bounds:
        ax.set_plot_bounds(**spatial_bounds)

    ax.set_xticks([])
    ax.set_yticks([])

    return ax


def add_colorbar2(im, ax, aspect=20, pad_fraction=0.1, drop_ax=False, **kwargs):
    """
    Add a vertical color bar to an image plot. Workaround for smopy_plot_helper figures
    :param im: The axes object to be represented in the colorbar
    :param ax: initial axes object
    :param aspect:
    :param pad_fraction:
    :param kwargs:
    :return:
    """
    bbox = ax.get_position()
    width = bbox.width
    height = bbox.height
    ax2 = ax.figure.add_axes([bbox.x1, bbox.y0, width * 1. / aspect, height],
                             label='twin', frameon=True, sharey=ax)

    ax2.xaxis.set_visible(False)
    divider = axes_grid1.make_axes_locatable(ax2.axes)
    width = axes_grid1.axes_size.AxesY(im.axes, aspect=1./aspect)
    pad = axes_grid1.axes_size.Fraction(pad_fraction, width)
    current_ax = plt.gca()
    ax2 = divider.append_axes("right", size=width, pad=pad)
    plt.sca(current_ax)
    cb = im.axes.figure.colorbar(im, cax=ax2, **kwargs)
    if drop_ax:
        ax2.figure.axes[1].remove()
        ax2.figure.axes[0].remove()
    return cb

