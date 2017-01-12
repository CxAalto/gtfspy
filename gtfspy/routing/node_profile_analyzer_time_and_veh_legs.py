from __future__ import print_function

from collections import defaultdict

import datetime
import matplotlib
import numpy
import matplotlib.pyplot as plt
from matplotlib import lines

from matplotlib import dates as md
from matplotlib.colors import ListedColormap, LinearSegmentedColormap

from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTimeWithBoardingsCount, compute_pareto_front, LabelTimeSimple
from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime
from gtfspy.routing.node_profile_simple import NodeProfileSimple
from gtfspy.routing.profile_block_analyzer import ProfileBlock, ProfileBlockAnalyzer


def _check_for_no_labels_for_n_veh_counts(func):
    def wrapper(self):
        assert(isinstance(self, NodeProfileAnalyzerTimeAndVehLegs))
        if len(self._labels_within_time_frame) == 0:
            if self._walk_to_target_duration is None:
                return 0
            else:
                return float('nan')
        else:
            return func(self)
    return wrapper


def _if_no_labels_return_inf(func):
    def wrapper(self):
        if self._labels_within_time_frame:
            return func(self)
        else:
            return float('inf')
    return wrapper

def _truncate_colormap(cmap, minval=0.0, maxval=1.0, n=100):
    """
    Truncates a colormap to use.
    Code originall from http://stackoverflow.com/questions/18926031/how-to-extract-a-subset-of-a-colormap-as-a-new-colormap-in-matplotlib
    """
    new_cmap = LinearSegmentedColormap.from_list(
        'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=minval, b=maxval),
        cmap(numpy.linspace(minval, maxval, n))
    )
    return new_cmap


class NodeProfileAnalyzerTimeAndVehLegs:

    def __init__(self, node_profile, start_time_dep, end_time_dep):
        """
        Initialize the data structures required by

        Parameters
        ----------
        node_profile: NodeProfileMultiObjective
        """
        self.node_profile = node_profile
        assert(self.node_profile.label_class == LabelTimeWithBoardingsCount)
        self.start_time_dep = start_time_dep
        self.end_time_dep = end_time_dep
        self.all_labels = [label for label in node_profile.get_final_optimal_labels() if
                           (start_time_dep <= label.departure_time <= end_time_dep)]

        after_label_candidates = [label for label in node_profile.get_final_optimal_labels()
                        if (label.departure_time > self.end_time_dep)]
        after_label_candidates.sort(key=lambda el: (el.arrival_time_target, el.n_boardings))
        min_n_boardings_observed = float('inf')
        after_labels = []
        for candidate_after_label in after_label_candidates:
            if candidate_after_label.n_boardings < min_n_boardings_observed:
                after_labels.append(candidate_after_label)
                min_n_boardings_observed = candidate_after_label.n_boardings


        self.all_labels.extend(after_labels)
        if len(after_labels) is 0:
            self._labels_within_time_frame = self.all_labels
        else:
            self._labels_within_time_frame = self.all_labels[::-len(after_labels)]

        self._walk_to_target_duration = self.node_profile.get_walk_to_target_duration()
        self._n_boardings_to_simple_time_analyzers = {}
        self._transfers_on_fastest_paths_analyzer = self._get_transfers_on_fastest_path_analyzer()

    def _get_transfers_on_fastest_path_analyzer(self):
        labels = list(reversed(compute_pareto_front(self.all_labels, ignore_n_boardings=True)))

        # assert ordered:
        for i in range(len(labels) - 1):
            assert(labels[i].departure_time <= labels[i + 1].departure_time)

        previous_dep_time = self.start_time_dep
        profile_blocks = []
        for label in labels:
            if previous_dep_time > self.end_time_dep:
                break
            end_time = min(label.departure_time, self.end_time_dep)
            assert(end_time >= previous_dep_time)
            distance_start = label.duration() + (label.departure_time - previous_dep_time)
            if distance_start > self._walk_to_target_duration:
                split_point_x_computed = label.departure_time - (self._walk_to_target_duration - label.duration())
                split_point_x = min(split_point_x_computed, end_time)
                walk_block = ProfileBlock(start_time=previous_dep_time, end_time=split_point_x, distance_start=0, distance_end=0)
                assert (previous_dep_time <= split_point_x)
                profile_blocks.append(walk_block)
                if split_point_x < end_time:
                    assert(split_point_x <= end_time)
                    trip_block = ProfileBlock(start_time=split_point_x, end_time=end_time,
                                              distance_start=label.n_boardings, distance_end=label.n_boardings)
                    profile_blocks.append(trip_block)
            else:
                journey_block = ProfileBlock(start_time=previous_dep_time, end_time=end_time,
                                 distance_start=label.n_boardings, distance_end=label.n_boardings)
                profile_blocks.append(journey_block)
            previous_dep_time = profile_blocks[-1].end_time
        if previous_dep_time < self.end_time_dep:
            if self._walk_to_target_duration < float('inf'):
                n_boardings = 0
            else:
                n_boardings = float('inf')
            profile_blocks.append(ProfileBlock(start_time=previous_dep_time, end_time=self.end_time_dep,
                                  distance_start=n_boardings, distance_end=n_boardings))
        return ProfileBlockAnalyzer(profile_blocks)

    def min_n_boardings(self):
        if self._walk_to_target_duration < float('inf'):
            return 0
        else:
            if len(self._labels_within_time_frame) is 0:
                return float('inf')
            else:
                return min([label.n_boardings for label in self._labels_within_time_frame])

    def min_n_boardings_on_shortest_paths(self):
        return self._transfers_on_fastest_paths_analyzer.min()

    def max_n_boardings_on_shortest_paths(self):
        return self._transfers_on_fastest_paths_analyzer.max()

    def mean_n_boardings_on_shortest_paths(self):
        return self._transfers_on_fastest_paths_analyzer.mean()

    def median_n_boardings_on_shortest_paths(self):
        return self._transfers_on_fastest_paths_analyzer.median()

    def _get_time_profile_analyzer(self, max_n_boardings=None):
        """
        Parameters
        ----------
        max_n_boardings: int

        Returns
        -------
        analyzer: NodeProfileAnalyzerTime
        """
        if max_n_boardings is None:
            max_n_boardings = self.max_trip_n_boardings()
        # compute only if not yet computed
        if not max_n_boardings in self._n_boardings_to_simple_time_analyzers:
            if max_n_boardings == 0:
                valids = []
            else:
                candidate_labels = [LabelTimeSimple(label.departure_time, label.arrival_time_target)
                                    for label in self.node_profile.get_final_optimal_labels() if
                                    ((self.start_time_dep <= label.departure_time)
                                     and label.n_boardings <= max_n_boardings)]
                valids = compute_pareto_front(candidate_labels)
            valids.sort(key=lambda label: -label.departure_time)
            profile = NodeProfileSimple(self._walk_to_target_duration)
            for valid in valids:
                profile.update_pareto_optimal_tuples(valid)
            npat = NodeProfileAnalyzerTime(profile, self.start_time_dep, self.end_time_dep)
            self._n_boardings_to_simple_time_analyzers[max_n_boardings] = npat
        return self._n_boardings_to_simple_time_analyzers[max_n_boardings]

    @_check_for_no_labels_for_n_veh_counts
    def max_trip_n_boardings(self):
        return numpy.max([label.n_boardings for label in self._labels_within_time_frame])

    @_check_for_no_labels_for_n_veh_counts
    def min_trip_n_boardings(self):
        values = [label.n_boardings for label in self._labels_within_time_frame]
        min_val = numpy.min(values)
        return min_val

    @_check_for_no_labels_for_n_veh_counts
    def mean_trip_n_boardings(self):
        return numpy.mean([label.n_boardings for label in self._labels_within_time_frame])

    @_check_for_no_labels_for_n_veh_counts
    def median_trip_n_boardings(self):
        return numpy.median([label.n_boardings for label in self._labels_within_time_frame])

    @_check_for_no_labels_for_n_veh_counts
    def temporal_mean_n_boardings(self):
        return numpy.median([label.n_boardings for label in self._labels_within_time_frame])

    @_if_no_labels_return_inf
    def min_temporal_distance(self):
        result = self._get_time_profile_analyzer().min_temporal_distance()
        assert(result >= 0), result
        return result

    @_if_no_labels_return_inf
    def max_temporal_distance(self):
        return self._get_time_profile_analyzer().max_temporal_distance()

    @_if_no_labels_return_inf
    def median_temporal_distance(self):
        return self._get_time_profile_analyzer().median_temporal_distance()

    @_if_no_labels_return_inf
    def mean_temporal_distance(self):
        return self._get_time_profile_analyzer().mean_temporal_distance()

    @_if_no_labels_return_inf
    def min_trip_duration(self):
        return self._get_time_profile_analyzer().min_trip_duration()

    @_if_no_labels_return_inf
    def max_trip_duration(self):
        return self._get_time_profile_analyzer().max_trip_duration()

    @_if_no_labels_return_inf
    def median_trip_duration(self):
        return self._get_time_profile_analyzer().median_trip_duration()

    @_if_no_labels_return_inf
    def mean_trip_duration(self):
        return self._get_time_profile_analyzer().mean_trip_duration()

    def mean_temporal_distance_with_min_n_boardings(self):
        min_n_boardings = self.min_n_boardings()
        min_n_boardings_analyzer = self._get_time_profile_analyzer(min_n_boardings)
        return min_n_boardings_analyzer.mean_temporal_distance()

    def min_temporal_distance_with_min_n_boardings(self):
        min_n_boardings = self.min_n_boardings()
        min_n_boardings_analyzer = self._get_time_profile_analyzer(min_n_boardings)
        return min_n_boardings_analyzer.min_temporal_distance()

    def median_temporal_distances(self, min_n_boardings=None, max_n_boardings=None):
        """
        Returns
        -------
        mean_temporal_distances: list
            list indices encode the number of vehicle legs each element
            in the list tells gets the mean temporal distance
        """
        if min_n_boardings is None:
            min_n_boardings = 0

        if max_n_boardings is None:
            max_n_boardings = self.max_trip_n_boardings()
            if max_n_boardings is None:
                max_n_boardings = 0

        median_temporal_distances = [float('inf') for _ in range(min_n_boardings, max_n_boardings + 1)]
        for n_boardings in range(min_n_boardings, max_n_boardings + 1):
            simple_analyzer = self._get_time_profile_analyzer(n_boardings)
            median_temporal_distances[n_boardings] = simple_analyzer.median_temporal_distance()
        return median_temporal_distances

    @classmethod
    def _get_colors_for_boardings(cls, min_n_boardings, max_n_boardings):
        cmap = NodeProfileAnalyzerTimeAndVehLegs.get_colormap_for_boardings(max_n_boardings)
        colors = [cmap(float(n_boardings)/max_n_boardings) for n_boardings in range(int(max_n_boardings) + 1)]
        return colors[min_n_boardings:max_n_boardings + 1]

    @classmethod
    def get_colormap_for_boardings(cls, max_n_boardings=None):
        n_default = 5
        if max_n_boardings in [float('nan'), None]:
            max_n_boardings = n_default
        from matplotlib import cm
        cmap = cm.get_cmap("cubehelix_r")
        start = 0.15
        end = 0.8
        if max_n_boardings is 0:
            step = 0
        else:
            divider = max(n_default, max_n_boardings)
            step = (end - start) / divider
        print(start + step * max_n_boardings)
        truncated = _truncate_colormap(cmap, start, start + step * max_n_boardings)
        return truncated

    def plot_temporal_distance_variation(self, timezone=None):
        """
        Parameters
        ----------
        timezone: str, optional

        Returns
        -------
        fig: matplotlib.Figure or None
            returns None, if there essentially is no profile to plot
        """
        max_n = self.max_trip_n_boardings()
        min_n = 0
        if max_n is None:
            return None
        fig = plt.figure()
        ax = fig.add_subplot(111)
        colors = NodeProfileAnalyzerTimeAndVehLegs._get_colors_for_boardings(min_n, max_n)
        max_temporal_distance = 0
        for color, n_boardings in zip(colors, range(min_n, max_n + 1)):
            npat = self._get_time_profile_analyzer(n_boardings)
            maxdist = npat.largest_finite_temporal_distance()
            if maxdist is not None and maxdist > max_temporal_distance:
                max_temporal_distance = maxdist
            linewidth = 0.5 + 3 * (n_boardings / max(1.0, float(max_n)))
            if n_boardings == max_n:
                label = "fastest possible using at most " + str(n_boardings) + " vehicle(s)"
            else:
                label = "time lost using " + str(n_boardings) + " vehicle(s) instead of " + str(n_boardings + 1)

            npat.plot_temporal_distance_profile(timezone=timezone,
                                                color=color,
                                                alpha=0.6,
                                                ax=ax,
                                                lw=linewidth,
                                                label=label)
        ax.legend(loc="best", framealpha=0.5)
        ax.set_ylim(bottom=0, top=max_temporal_distance / 60.0 * 1.05)
        return fig

    @classmethod
    def _multiply_color_saturation(cls, color, multiplier):
        hsv = matplotlib.colors.rgb_to_hsv(color[:3])
        rgb = matplotlib.colors.hsv_to_rgb((hsv[0], hsv[1] * multiplier, hsv[2]))
        return list(iter(rgb)) + [1]

    @classmethod
    def _multiply_color_brightness(cls, color, multiplier):
        hsv = matplotlib.colors.rgb_to_hsv(color[:3])
        rgb = matplotlib.colors.hsv_to_rgb((hsv[0], hsv[1], max(0, min(1, hsv[2]*multiplier))))
        return list(iter(rgb)) + [1]


    def plot_new_transfer_temporal_distance_profile(self,
                                                    timezone=None,
                                                    format_string="%Y-%m-%d %H:%M:%S",
                                                    duration_divider=60.0,
                                                    ax=None,
                                                    plot_journeys=True,
                                                    highlight_fastest_path=True,
                                                    default_lw=5,
                                                    ncol_legend=1,
                                                    fastest_path_lw=3   ):
        max_n = self.max_trip_n_boardings()
        min_n = self.min_trip_n_boardings()
        print(max_n, min_n)

        if self._walk_to_target_duration < float('inf'):
            min_n = 0
        if max_n is None:
            return None
        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot(111)
        fig = ax.figure
        assert(isinstance(ax, matplotlib.axes.Axes))

        def _ut_to_unloc_datetime(ut):
            dt = datetime.datetime.fromtimestamp(ut, timezone)
            return dt.replace(tzinfo=None)

        if format_string:
            x_axis_formatter = md.DateFormatter(format_string)
            ax.xaxis.set_major_formatter(x_axis_formatter)
        else:
            _ut_to_unloc_datetime = lambda x: x

        ax.set_xlim(
            _ut_to_unloc_datetime(self.start_time_dep),
            _ut_to_unloc_datetime(self.end_time_dep)
        )

        colors = self._get_colors_for_boardings(min_n, max_n)
        n_boardings_range = range(min_n, max_n + 1)
        nboardings_to_lw = {n: default_lw for i, n in enumerate(n_boardings_range)}
        nboardings_to_color = {n:colors[i] for i, n in enumerate(n_boardings_range)}

        n_boardings_to_line_color = {}
        n_boardings_to_fill_color = {}

        #
        rgbs = [color_tuple[:3] for color_tuple in nboardings_to_color.values()]
        hsvs = matplotlib.colors.rgb_to_hsv(rgbs)
        max_saturation = max([hsv[1] for hsv in hsvs])
        line_saturation_multiplier = 1/max_saturation

        for n, color_tuple in nboardings_to_color.items():
            c = NodeProfileAnalyzerTimeAndVehLegs._multiply_color_saturation(color_tuple, line_saturation_multiplier)
            c = NodeProfileAnalyzerTimeAndVehLegs._multiply_color_brightness(c, 1.2)
            n_boardings_to_line_color[n] = c

            c = NodeProfileAnalyzerTimeAndVehLegs._multiply_color_brightness(color_tuple, 1.2)
            c = NodeProfileAnalyzerTimeAndVehLegs._multiply_color_saturation(c, 0.8)
            print(c)
            n_boardings_to_fill_color[n] = c


        #  get all trips ordered by departure time


        deptime_ordered_labels = sorted(list(self.all_labels), key=lambda x: x.departure_time)

        n_boardings_to_labels = defaultdict(list)
        for journey_label in deptime_ordered_labels:
            n_boardings_to_labels[journey_label.n_boardings].append(journey_label)

        walk_duration = self._walk_to_target_duration / duration_divider
        if walk_duration < float('inf'):
            xs = [_ut_to_unloc_datetime(x) for x in [self.start_time_dep, self.end_time_dep]]
            ax.plot(xs, [walk_duration, walk_duration], lw=nboardings_to_lw[0], color=n_boardings_to_line_color[0])
            ax.fill_between(xs, 0, walk_duration, color=n_boardings_to_fill_color[0])
            max_tdist = walk_duration
        else:
            max_tdist = 0

        journeys = []
        for n_boardings in n_boardings_range:
            if n_boardings == 0:
                # dealt above
                continue
            prev_analyzer = self._get_time_profile_analyzer(n_boardings - 1)
            prev_profile_block_analyzer = prev_analyzer.temporal_profile_analyzer
            assert(isinstance(prev_profile_block_analyzer, ProfileBlockAnalyzer))
            labels = n_boardings_to_labels[n_boardings]
            for i, journey_label in enumerate(labels):
                if journey_label.departure_time > self.end_time_dep and journey_label.n_boardings == 3:
                    continue
                prev_dep_time = self.start_time_dep
                if i is not 0:
                    prev_dep_time = labels[i - 1].departure_time
                # this could perhaps be made a while loop of some sort
                # to not loop over things multiple times
                for block in prev_profile_block_analyzer._profile_blocks:
                    if block.distance_end != block.distance_start:
                        if block.distance_end < journey_label.duration() + (journey_label.departure_time - block.end_time):
                            prev_dep_time = max(prev_dep_time, block.end_time)
                    elif block.distance_end == block.distance_start:
                        # look for the time when
                        waiting_time = (block.distance_end - journey_label.duration())
                        prev_dep_time = max(prev_dep_time, journey_label.departure_time - waiting_time)
                # prev dep time is now known
                waiting_time = journey_label.departure_time - prev_dep_time
                color = nboardings_to_color[n_boardings]
                lw = nboardings_to_lw[n_boardings]
                xs = [_ut_to_unloc_datetime(prev_dep_time), _ut_to_unloc_datetime(journey_label.departure_time)]
                ys = numpy.array([journey_label.duration() + waiting_time, journey_label.duration()]) / duration_divider
                max_tdist = max(ys[0], max_tdist)
                ax.plot(xs, ys,
                        color=n_boardings_to_line_color[n_boardings], # "k",
                        lw=lw)
                ax.fill_between(xs, 0, ys, color=color)
                ax.fill_between(xs, 0, ys, color=n_boardings_to_fill_color[n_boardings])
                if plot_journeys:
                    journeys.append((xs[1], ys[1]))

        legend_patches = []
        for n_boardings in n_boardings_range:
            text = u'{d} boardings'.format(d=n_boardings)
            if n_boardings == 1:
                text = text[:-1]
            if n_boardings == 0:
                text = u"walk"
            p = lines.Line2D([0, 1], [0, 1], lw=nboardings_to_lw[n_boardings],
                             color=n_boardings_to_line_color[n_boardings],
                             label=text)
            legend_patches.append(p)

        if highlight_fastest_path:
            fastest_path_time_analyzer = self._get_time_profile_analyzer()
            vlines, slopes = fastest_path_time_analyzer.temporal_profile_analyzer.get_vlines_and_slopes_for_plotting()
            lw = fastest_path_lw
            ls = "--"
            for vline in vlines:
                ax.plot([_ut_to_unloc_datetime(x) for x in vline['x']], numpy.array(vline['y']) / duration_divider,
                        ls=ls, lw=lw, color="k")
            for slope in slopes:
                ax.plot([_ut_to_unloc_datetime(x) for x in slope['x']], numpy.array(slope['y']) / duration_divider,
                        ls=ls, color="k", lw=lw)
            p = lines.Line2D([0, 1], [0, 1], ls=ls, lw=lw, color="k", label="fastest path profile")
            legend_patches.append(p)



        if plot_journeys:
            letters = iter("ABCDEFGHIJKLMN")
            journeys.sort()
            for (x, y), letter in zip(journeys, letters):
                if x < _ut_to_unloc_datetime(self.end_time_dep):
                    ax.plot(x, y, "o", ms=8, color="k")
                    ax.text(x + datetime.timedelta(seconds=0.2),
                            y / duration_divider - 0.9, letter, va="center", ha="left")
            p = lines.Line2D([0, 0], [1, 1], ls="", marker="o", ms=8, color="k", label="journeys")
            legend_patches.append(p)

        ax.legend(handles=legend_patches, loc="best", ncol=ncol_legend)
        ax.set_ylim(0, 1.1 * max_tdist)
        ax.set_xlabel("Departure time")
        ax.set_ylabel("Temporal distance")
        return fig


    def plot_fastest_temporal_distance_profile(self, timezone=None, **kwargs):
        max_n = self.max_trip_n_boardings()
        if "ax" not in kwargs:
            fig = plt.figure(figsize=(10, 6))
            ax = fig.add_subplot(111)
            kwargs["ax"] = ax
        npat = self._get_time_profile_analyzer(max_n)
        fig = npat.plot_temporal_distance_profile(timezone=timezone,
                                                  **kwargs)
        return fig

    def n_pareto_optimal_trips(self):
        """
        Get number of pareto-optimal trips

        Returns
        -------
        n_trips: float
        """
        return float(len(self._labels_within_time_frame))

    @staticmethod
    def all_measures_and_names_as_lists():
        NPA = NodeProfileAnalyzerTimeAndVehLegs
        profile_summary_methods = [
            NPA.max_trip_duration,
            NPA.mean_trip_duration,
            NPA.median_trip_duration,
            NPA.min_trip_duration,
            NPA.max_temporal_distance,
            NPA.mean_temporal_distance,
            NPA.median_temporal_distance,
            NPA.min_temporal_distance,
            NPA.n_pareto_optimal_trips,
            NPA.min_n_boardings,
            NPA.min_trip_n_boardings,
            NPA.max_trip_n_boardings,
            NPA.mean_trip_n_boardings,
            NPA.median_trip_n_boardings,
            NPA.mean_n_boardings_on_shortest_paths,
            NPA.min_n_boardings_on_shortest_paths,
            NPA.max_n_boardings_on_shortest_paths,
            NPA.median_n_boardings_on_shortest_paths,
            NPA.mean_temporal_distance_with_min_n_boardings,
            NPA.min_temporal_distance_with_min_n_boardings
        ]
        profile_observable_names = [
            "max_trip_duration",
            "mean_trip_duration",
            "median_trip_duration",
            "min_trip_duration",
            "max_temporal_distance",
            "mean_temporal_distance",
            "median_temporal_distance",
            "min_temporal_distance",
            "n_pareto_optimal_trips",
            "min_n_boardings",
            "min_trip_n_boardings",
            "max_trip_n_boardings",
            "mean_trip_n_boardings",
            "median_trip_n_boardings",
            "mean_n_boardings_on_shortest_paths",
            "min_n_boardings_on_shortest_paths",
            "max_n_boardings_on_shortest_paths",
            "median_n_boardings_on_shortest_paths",
            "mean_temporal_distance_with_min_n_boardings",
            "min_temporal_distance_with_min_n_boardings"
        ]
        assert(len(profile_summary_methods) == len(profile_observable_names))
        return profile_summary_methods, profile_observable_names
