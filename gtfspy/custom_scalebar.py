from matplotlib_scalebar.scalebar import ScaleBar
# Standard library modules.
import bisect

# Third party modules.
import matplotlib
from matplotlib.text import Text
from matplotlib.artist import Artist
from matplotlib.font_manager import FontProperties
from matplotlib.rcsetup import \
    (defaultParams, validate_float, validate_legend_loc, validate_bool,
     validate_color, ValidateInStrings)
from matplotlib.offsetbox import \
    AuxTransformBox, TextArea, VPacker, HPacker, AnchoredOffsetbox
from matplotlib.patches import Rectangle, FancyArrow
from matplotlib.lines import Line2D

import six

# Local modules.
from matplotlib_scalebar.dimension import \
    (_Dimension, SILengthDimension, SILengthReciprocalDimension,
     ImperialLengthDimension)


class CustomScaleBar(ScaleBar):
    def __init__(self, n_fields=5, *args, **kwargs):
        super(CustomScaleBar, self).__init__(*args, **kwargs)
        self.n_fields = n_fields

    def draw(self, renderer, *args, **kwargs):
        if not self.get_visible():
            return
        if self.dx == 0:
            return

        # Get parameters
        from matplotlib import rcParams  # late import

        def _get_value(attr, default):
            value = getattr(self, attr)
            if value is None:
                value = rcParams.get('scalebar.' + attr, default)
            return value

        length_fraction = _get_value('length_fraction', 0.4)
        height_fraction = _get_value('height_fraction', 0.01)
        location = _get_value('location', 'upper right')
        if isinstance(location, six.string_types):
            location = self._LOCATIONS[location]
        pad = _get_value('pad', 0.2)
        border_pad = _get_value('border_pad', 0.1)
        sep = _get_value('sep', 5)
        frameon = _get_value('frameon', False)
        color = _get_value('color', 'k')
        sec_color = 'w'
        box_color = _get_value('box_color', 'w')
        box_alpha = _get_value('box_alpha', 1.0)
        scale_loc = _get_value('scale_loc', 'bottom')
        label_loc = _get_value('label_loc', 'top')
        font_properties = self.font_properties
        fixed_value = self.fixed_value
        fixed_units = self.fixed_units or self.units

        if font_properties is None:
            textprops = {'color': color}
        else:
            textprops = {'color': color, 'fontproperties': font_properties}

        ax = self.axes
        xlim = ax.get_xlim()
        ylim = ax.get_ylim()
        label = self.label

        # Calculate value, units and length
        # Mode 1: Auto
        if self.fixed_value is None:
            length_px = abs(xlim[1] - xlim[0]) * length_fraction
            length_px, value, units = self._calculate_best_length(length_px)

        # Mode 2: Fixed
        else:
            value = fixed_value
            units = fixed_units
            length_px = self._calculate_exact_length(value, units)

        scale_label = self.label_formatter(value, self.dimension.to_latex(units))

        size_vertical = abs(ylim[1] - ylim[0]) * height_fraction

        # Create size bar
        sizebar = AuxTransformBox(ax.transData)
        increment = length_px / self.n_fields
        label_increment = value / self.n_fields
        font_size_multiplier = 7
        linewidth = 0.5
        north_arrow = True
        na_x = length_px * 1
        na_y = size_vertical * -5
        na_length = size_vertical * -6
        na_offset = size_vertical * -2

        style = "zigzag"
        # TODO: somehow take into account the length of the figures to determine needed space
        font_size = 7  # for some reason font size does not scale with the figure
        for n in range(self.n_fields):
            if style == "rectangles":
                sizebar.add_artist(Rectangle((n * increment, 0), increment, size_vertical,
                                             fill=True, facecolor=color if n % 2 else sec_color, edgecolor=None))
            elif style == "zigzag":
                sizebar.add_artist(Line2D((n * increment, n * increment), (0, size_vertical),
                                          color=color, linewidth=linewidth))
                y = size_vertical if n % 2 else 0
                sizebar.add_artist(Line2D((n * increment, (n + 1) * increment), (y, y),
                                          color=color, linewidth=linewidth))
            sizebar.add_artist(Text(n * increment, size_vertical*4, str(int(n * label_increment)),
                                    fontsize=font_size, color=color, horizontalalignment="center"))

        if style == "zigzag":
            sizebar.add_artist(Line2D((length_px, length_px), (0, size_vertical), color=color, linewidth=linewidth))

        sizebar.add_artist(Text(length_px, size_vertical*4, str(value),
                                fontsize=font_size, color=color, horizontalalignment="center"))
        sizebar.add_artist(Text(length_px + increment, size_vertical*4, units,
                                fontsize=font_size, color=color, horizontalalignment="center"))

        if north_arrow:
            sizebar.add_artist(FancyArrow(na_x, na_y, 0, na_length, color=color, head_width=size_vertical*2,
                                          length_includes_head=True))
            sizebar.add_artist(Text(na_x, na_y+na_length+na_offset, "N",
                                    fontsize=font_size,
                                    color=color, horizontalalignment="center"))

        txtscale = TextArea(scale_label, minimumdescent=False, textprops=textprops)

        if style:
            children = [sizebar]
        else:
            if scale_loc in ['bottom', 'right']:
                children = [sizebar, txtscale]
            else:
                children = [txtscale, sizebar]
        if scale_loc in ['bottom', 'top']:
            Packer = VPacker
        else:
            Packer = HPacker
        boxsizebar = Packer(children=children, align='center', pad=0, sep=sep)

        # Create text area
        if label:
            txtlabel = TextArea(label, minimumdescent=False, textprops=textprops)
        else:
            txtlabel = None

        # Create final offset box
        if txtlabel:
            if label_loc in ['bottom', 'right']:
                children = [boxsizebar, txtlabel]
            else:
                children = [txtlabel, boxsizebar]
            if label_loc in ['bottom', 'top']:
                Packer = VPacker
            else:
                Packer = HPacker
            child = Packer(children=children, align='center', pad=0, sep=sep)
        else:
            child = boxsizebar

        box = AnchoredOffsetbox(loc=location,
                                pad=pad,
                                borderpad=border_pad,
                                child=child,
                                frameon=frameon)

        box.axes = ax
        box.set_figure(self.get_figure())
        box.patch.set_color(box_color)
        box.patch.set_alpha(box_alpha)
        box.draw(renderer)
