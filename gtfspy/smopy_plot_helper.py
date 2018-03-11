from matplotlib.axes import Axes
from matplotlib.projections import register_projection
from urllib.error import URLError
import smopy
import numpy
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


class SmopyAxes(Axes):
    """
    Subclass of Axes, that
    """

    name = 'smopy_axes'

    def __init__(self, *args, **kwargs):
        super(SmopyAxes, self).__init__(*args, **kwargs)
        self.smopy_map = None
        self.min_lon = None
        self.max_lon = None
        self.min_lat = None
        self.max_lat = None
        self.map_fixed = False
        self.maps = {}
        self.prev_plots = []
        self.prev_scatter = []
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
                self.prev_scatter.append((lons, lats, dict(**kwargs)))

        _x, _y = self.smopy_map.to_pixels(lats, lons)
        super().scatter(_x, _y, **kwargs)

    def plot(self, lons, lats, update=True, **kwargs):
        if not hasattr(lats, '__iter__'):
            lats = [lats]
            lons = [lons]
        lons = numpy.array(lons)
        lats = numpy.array(lats)
        if update:
            if not self.smopy_map or not self.map_fixed:
                self.smopy_map = self._get_smopy_map_from_coords(lons, lats)
                self.prev_plots.append((lons, lats, dict(**kwargs)))

        _x, _y = self.smopy_map.to_pixels(lats, lons)
        super().plot(_x, _y, **kwargs)

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
        super().text(_x, _y, s, **kwargs)

    def _get_smopy_map_from_coords(self, lons, lats, **kwargs):

        min_lon, max_lon, min_lat, max_lat = self.min_lon, self.max_lon, self.min_lat, self.max_lat
        self.min_lon = min(list(lons) + [min_lon]) if min_lon else min(list(lons))
        self.min_lat = min(list(lats) + [min_lat]) if min_lat else min(list(lats))
        self.max_lon = max(list(lons) + [max_lon]) if max_lon else max(list(lons))
        self.max_lat = max(list(lats) + [max_lat]) if max_lat else max(list(lats))
        if not all([min_lon == self.min_lon,
                    min_lat == self.min_lat,
                    max_lon == self.max_lon,
                    max_lat == self.max_lat]):
            self.smopy_map = self._init_smopy_map(self.min_lon, self.max_lon, self.min_lat, self.max_lat, **kwargs)
            self.update_plots()
            super().imshow(self.smopy_map.to_pil())


        return self.smopy_map

    def update_plots(self):
        self.clear()
        for (lons, lats, kwords) in self.prev_plots:
            self.plot(lons, lats, update=False, **kwords)
        for (lons, lats, kwords) in self.prev_scatter:
            self.scatter(lons, lats, update=False, **kwords)
        for (lons, lats, s, kwords) in self.prev_text:
            self.text(lons, lats, s, update=False, **kwords)

    def _init_smopy_map(self, lon_min, lon_max, lat_min, lat_max, z=None, map_style="dark_nolabels"):

        ORIG_TILE_SERVER = smopy.TILE_SERVER
        if map_style is not None:
            assert map_style in MAP_STYLES, \
                map_style + " (map_style parameter) is not a valid CartoDB mapping style. " \
                            "Options are " + str(MAP_STYLES)
            smopy.TILE_SERVER = "http://1.basemaps.cartocdn.com/" + map_style + "/{z}/{x}/{y}.png"

        args = (lat_min, lat_max, lon_min, lon_max, map_style, z)
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
        smopy.TILE_SERVER = ORIG_TILE_SERVER
        return self.maps[args]

    def set_spatial_bounds(self, min_lon, max_lon, min_lat, max_lat, **kwargs):
        self.smopy_map = self._init_smopy_map(min_lon, max_lon, min_lat, max_lat, **kwargs)
        self.map_fixed = True
        super().imshow(self.smopy_map.to_pil())

    def plot_line_segments(self, from_lons, from_lats, to_lons, to_lats, width_attributes=None, color_attributes=None,
                           zorders=None, **kwargs):
        # TODO: to make this compatible, segment coords should be converted to lons = [lon1, lon2], lats = [lat1, lat2]
        self.set_spatial_bounds(min(from_lons+to_lons), max(from_lons+to_lons),
                                min(from_lats+to_lats), max(from_lats+to_lats))
        for from_lon, from_lat, to_lon, to_lat, width_attribute, color_attribute, zorder in zip(from_lons,
                                                                                                from_lats,
                                                                                                to_lons,
                                                                                                to_lats,
                                                                                                width_attributes,
                                                                                                color_attributes,
                                                                                                zorders):

            self.plot(numpy.array([from_lat, to_lat]), numpy.array([from_lon, to_lon]),
                      color=color_attribute,
                      linewidth=width_attribute,
                      zorder=zorder,
                      **kwargs)

register_projection(SmopyAxes)
