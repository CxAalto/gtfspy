import matplotlib.lines as mlines
import numpy
import smopy
from contextlib import contextmanager
from matplotlib.axes import Axes
from matplotlib.projections import register_projection
from matplotlib.collections import LineCollection
import matplotlib.lines as mlines
from matplotlib_scalebar.scalebar import ScaleBar
from urllib.error import URLError
from shapely.geometry import LineString
import smopy
import numpy

from gtfspy import util
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR, ROUTE_TYPE_TO_SHORT_DESCRIPTION
from gtfspy.custom_scalebar import CustomScaleBar

MAP_STYLE_OSM_DEFAULT = "openstreetmap_default"
MAP_STYLE_LIGHT_ALL = "light_all"
MAP_STYLE_DARK_ALL = "dark_all"

MAP_STYLES = [
    MAP_STYLE_OSM_DEFAULT,
    "rastertiles/voyager",
    "rastertiles/voyager_nolabels",
    "rastertiles/voyager_only_labels",
    "rastertiles/voyager_labels_under",
    MAP_STYLE_LIGHT_ALL,
    MAP_STYLE_DARK_ALL,
    "light_nolabels",
    "light_only_labels",
    "dark_nolabels",
    "dark_only_labels"
]


def legend_pt_modes(ax, route_types, **kwargs):
    unique_types = set(route_types)
    lines = []

    for i in unique_types:
        if i == "wait":
            line = mlines.Line2D([], [], linestyle=':', color="black", markersize=15,
                                 label="Waiting time")
        else:
            line = mlines.Line2D([], [], color=ROUTE_TYPE_TO_COLOR[i], markersize=15,
                                 label=ROUTE_TYPE_TO_SHORT_DESCRIPTION[i])

        lines.append(line)
    handles = lines
    labels = [h.get_label() for h in handles]

    ax.legend(handles=handles, labels=labels, **kwargs)
    return ax


class SmopyAxes(Axes):
    """
    Subclass of Axes, that
    """

    name = 'smopy_axes'

    def __init__(self, *args, **kwargs):
        super(SmopyAxes, self).__init__(*args, **kwargs)
        self.map_style = kwargs.pop("map_style", "light_nolabels")
        self.smopy_map = None
        self.lon_min = None
        self.lon_max = None
        self.lat_min = None
        self.lat_max = None
        self.map_fixed = False
        self.maps = {}
        self.prev_plots = []
        self.prev_scatters = []
        self.prev_text = []
        self.axes.get_xaxis().set_visible(False)
        self.axes.get_yaxis().set_visible(False)

    def scatter(self, lons, lats, update=True, **kwargs):
        if not hasattr(lats, '__iter__'):
            lats = [lats]
            lons = [lons]

        lons = numpy.array(lons)
        lats = numpy.array(lats)
        if update:
            if not self.smopy_map or not self.map_fixed:
                self.smopy_map = self._get_smopy_map_from_coords(lons, lats)
                self.prev_scatters.append((lons, lats, dict(**kwargs)))

        _x, _y = self.smopy_map.to_pixels(lats, lons)
        return super().scatter(_x, _y, **kwargs)

    def plot(self, lons, lats, update=True, **kwargs):
        if not hasattr(lats, '__iter__'):
            lats = [lats]
            lons = [lons]
        lons = numpy.array(lons)
        lats = numpy.array(lats)
        if update:
            if not (self.smopy_map and self.map_fixed):
                self.smopy_map = self._get_smopy_map_from_coords(lons, lats)
                self.prev_plots.append((lons, lats, dict(**kwargs)))

        _x, _y = self.smopy_map.to_pixels(lats, lons)
        return super().plot(_x, _y, **kwargs)

    def text(self, lons, lats, s, update=True, **kwargs):
        if not hasattr(lats, '__iter__'):
            lats = [lats]
            lons = [lons]

        lons = numpy.array(lons)
        lats = numpy.array(lats)
        if update:
            if not self.smopy_map or not self.map_fixed:
                self.smopy_map = self._get_smopy_map_from_coords(lons, lats)
                self.prev_text.append((lons, lats, s, dict(**kwargs)))

        _x, _y = self.smopy_map.to_pixels(lats, lons)
        return super().text(_x, _y, s, **kwargs)

    def _get_smopy_map_from_coords(self, lons, lats, **kwargs):
        lon_min, lon_max, lat_min, lat_max = self.lon_min, self.lon_max, self.lat_min, self.lat_max
        self.lon_min = min(list(lons) + [lon_min]) if lon_min else min(list(lons))
        self.lat_min = min(list(lats) + [lat_min]) if lat_min else min(list(lats))
        self.lon_max = max(list(lons) + [lon_max]) if lon_max else max(list(lons))
        self.lat_max = max(list(lats) + [lat_max]) if lat_max else max(list(lats))
        if not all([lon_min == self.lon_min,
                    lat_min == self.lat_min,
                    lon_max == self.lon_max,
                    lat_max == self.lat_max]):
            self.smopy_map = self._init_smopy_map(self.lon_min, self.lon_max, self.lat_min, self.lat_max, **kwargs)
            self.update_plots()
            super().imshow(self.smopy_map.to_pil())

        return self.smopy_map

    def update_plots(self):
        self.clear()
        for (lons, lats, kwords) in self.prev_plots:
            self.plot(lons, lats, update=False, **kwords)
        for (lons, lats, kwords) in self.prev_scatters:
            self.scatter(lons, lats, update=False, **kwords)
        for (lons, lats, s, kwords) in self.prev_text:
            self.text(lons, lats, s, update=False, **kwords)

    def _init_smopy_map(self, lon_min, lon_max, lat_min, lat_max, z=None):
        with using_smopy_map_style(self.map_style):
            args = (lat_min, lat_max, lon_min, lon_max, smopy.TILE_SERVER, z)
            if args not in self.maps:
                kwargs = {}
                if z is not None:  # this hack may not work
                    smopy.Map.get_allowed_zoom = lambda _self, _el: z
                    kwargs['z'] = z
                try:
                    self.maps[args] = smopy.Map((lat_min, lon_min, lat_max, lon_max), **kwargs)
                except URLError:
                    raise RuntimeError("\n Could not load background map from the tile server: " +
                                       smopy.TILE_SERVER +
                                       "\n Please check that the tile server exists and "
                                       "that your are connected to the internet.")
        return self.maps[args]

    def set_map_bounds(self, lon_min=None, lon_max=None, lat_min=None, lat_max=None):
        """
        Sets the bounds for the background map.

        Parameters
        ----------
        lon_min: float
        lon_max: float
        lat_min: float
        lat_max: float

        See also
        --------
        set_plot_bounds
        """

        self.lon_min, self.lon_max, self.lat_min, self.lat_max = lon_min, lon_max, lat_min, lat_max
        self.smopy_map = self._init_smopy_map(lon_min, lon_max, lat_min, lat_max)
        self.map_fixed = True
        super().imshow(self.smopy_map.to_pil())

    def set_plot_bounds(self, lon_min=None, lon_max=None, lat_min=None, lat_max=None):

        """
        Sets the plot bounds similar to ax.set_xlim() and ax.set_ylim()

        Parameters
        ----------
        lon_min: float
        lon_max: float
        lat_min: float
        lat_max: float

        See also
        --------
        set_map_bounds
        """
        assert self.smopy_map, "The smopy map needs to be intialized using set_map_bounds"
        assert all([lon_max, lon_min, lat_min, lat_max])
        xs, ys = self.smopy_map.to_pixels(numpy.array([lat_min, lat_max]),
                                          numpy.array([lon_min, lon_max]))
        self.set_xlim(xs)
        self.set_ylim(ys)

    def add_scalebar(self, **kwargs):
        distance_m = util.wgs84_distance(self.lat_min, self.lon_min, self.lat_min, self.lon_max)
        xs, ys = self.smopy_map.to_pixels(numpy.array([self.lat_min, self.lat_min]),
                                          numpy.array([self.lon_min, self.lon_max]))
        scalebar = CustomScaleBar(dx=distance_m / (xs.max() - xs.min()), **kwargs)
        self.add_artist(scalebar)

    def plot_line_segments(self, coords,
                           linewidths,
                           colors,
                           zorders,
                           update=True, **kwargs):
        """

        :param coords: list of coordinate trajectories eg: [(row.from_lon, row.from_lat), (row.to_lon, row.to_lat)]
        :param linewidths: list
        :param colors: list
        :param zorders:
        :param kwargs:
        :return:
        """
        lons = [x[0][0] for x in coords] + [x[1][0] for x in coords]
        lats = [x[0][1] for x in coords] + [x[1][1] for x in coords]
        lons = numpy.array(lons)
        lats = numpy.array(lats)

        if update:
            raise NotImplementedError
            #if not self.smopy_map or not self.map_fixed:
                #self.smopy_map = self._get_smopy_map_from_coords(lons, lats)
                #self.prev_plots.append((lons, lats, dict(**kwargs)))

        xy_coords = [[self.smopy_map.to_pixels(x[0][1], x[0][0]), self.smopy_map.to_pixels(x[1][1], x[1][0])] for x in coords]
        for zorder in set(zorders):

            lc = LineCollection([x for x, z in zip(xy_coords, zorders) if zorder == z],
                                linewidths=[x for x, z in zip(linewidths, zorders) if zorder == z],
                                color=[x for x, z in zip(colors, zorders) if zorder == z],
                                zorder=zorder, **kwargs)
            super().add_collection(lc)
        return

    def plot_geometry(self, geometries, **kwargs):
        if not hasattr(geometries, '__iter__'):
            geometries = [geometries]

        assert isinstance(geometries[0], LineString)
        lons = []
        lats = []
        for geometry in geometries:
            lats.append([segment[1] for segment in geometry.coords])
            lons.append([segment[0] for segment in geometry.coords])

        self.plot(lons, lats, **kwargs)


@contextmanager
def using_smopy_map_style(map_style):
    orig_tile_server = smopy.TILE_SERVER
    if map_style is not None:
        assert map_style in MAP_STYLES, \
            map_style + " (map_style parameter) is not a valid mapping style. " \
                        "Options are " + str(MAP_STYLES)
        if map_style == "openstreetmap_default":
            smopy.TILE_SERVER = "http://tile.openstreetmap.org/{z}/{x}/{y}.png"
        else:
            smopy.TILE_SERVER = "http://1.basemaps.cartocdn.com/" + map_style + "/{z}/{x}/{y}.png"

    yield
    smopy.TILE_SERVER = orig_tile_server


register_projection(SmopyAxes)
