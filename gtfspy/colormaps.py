import matplotlib.colors
import matplotlib.cm
import numpy

# colormaps: "viridis", "plasma_r","seismic"


def get_colormap(observable_name=None):
    if "diff_minutes" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-30, vmax=30)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_number" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-4, vmax=4)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_relative" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-0.7, vmax=0.7)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_multiples" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-2, vmax=2)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_simpson" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-0.5, vmax=0.5)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_n_trips" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-10, vmax=10)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "diff_n_routes" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=-5, vmax=5)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "simpson" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=0, vmax=1.0)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "n_trips" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=0, vmax=30)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    elif "n_routes" in observable_name:
        norm = matplotlib.colors.Normalize(vmin=0, vmax=15)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)

    else:
        norm = matplotlib.colors.Normalize(vmin=-30, vmax=30)
        cmap = matplotlib.cm.get_cmap(name="seismic", lut=None)
    return cmap, norm

