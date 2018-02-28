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
        self.lon_min = None
        self.lon_max = None
        self.lat_min = None
        self.lat_max = None
        self.maps = {}

    def scatter(self, lon, lat, **kwargs):
        if not hasattr(lat, '__iter__'):
            lat = [lat]
            lon = [lon]
        
        lon = numpy.array(lon)
        lat = numpy.array(lat)
        lon_min = min([x for x in lon])
        lon_max = max([x for x in lon])
        lat_min = min([x for x in lat])
        lat_max = max([x for x in lat])
        if not self.smopy_map:
            self.smopy_map = self._init_smopy_map(lon_min, lon_max, lat_min, lat_max, map_style="dark_nolabels")
        _x, _y = self.smopy_map.to_pixels(lat, lon)
        super().imshow(self.smopy_map.to_pil())
        super().scatter(_x, _y, **kwargs)

    def set_spatial_bounds(self):
        pass

    def ticklabel_format(self, **kwargs):
        super().ticklabel_format(**kwargs)

    def remove(self):
        super().remove()

    def _init_smopy_map(self, lon_min, lon_max, lat_min, lat_max, z=None, map_style=None):
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


register_projection(SmopyAxes)
