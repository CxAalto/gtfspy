import matplotlib.colors
import matplotlib.cm
import numpy

# colormaps: "viridis", "plasma_r","seismic"


def get_colormap(observable_name):
    if "diff_minutes" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-30, vmax=30)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_number" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-4, vmax=4)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_percentage" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-0.5, vmax=0.5)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    else:
        norm = matplotlib.colors.Normalize(vmin=-30, vmax=30)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    return cmap, norm

