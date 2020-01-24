import datetime
import warnings

import numpy
import pytz

from matplotlib import dates as md, rcParams
import matplotlib.pyplot as plt

from gtfspy.routing.profile_block_analyzer import ProfileBlockAnalyzer
from gtfspy.routing.profile_block import ProfileBlock
from gtfspy.routing.node_profile_simple import NodeProfileSimple


def _if_no_trips_return_inf(func):
    def wrapper(self):
        if self.trip_durations:
            return func(self)
        else:
            return float("inf")

    return wrapper


class NodeProfileAnalyzerTime:
    @classmethod
    def from_profile(cls, node_profile, start_time_dep, end_time_dep):
        assert isinstance(node_profile, NodeProfileSimple), type(node_profile)
        return NodeProfileAnalyzerTime(
            node_profile.get_final_optimal_labels(),
            node_profile.get_walk_to_target_duration(),
            start_time_dep,
            end_time_dep,
        )

    def __init__(self, labels, walk_time_to_target, start_time_dep, end_time_dep):
        """
        Initialize the data structures required by

        Parameters
        ----------
        node_profile: NodeProfileSimple

        """
        self.start_time_dep = start_time_dep
        self.end_time_dep = end_time_dep
        # used for computing temporal distances:
        all_pareto_optimal_tuples = [
            pt for pt in labels if (start_time_dep < pt.departure_time < end_time_dep)
        ]

        labels_after_dep_time = [
            label for label in labels if label.departure_time >= self.end_time_dep
        ]
        if labels_after_dep_time:
            next_label_after_end_time = min(
                labels_after_dep_time, key=lambda el: el.arrival_time_target
            )
            all_pareto_optimal_tuples.append(next_label_after_end_time)

        all_pareto_optimal_tuples = sorted(
            all_pareto_optimal_tuples, key=lambda ptuple: ptuple.departure_time
        )

        arrival_time_target_at_end_time = end_time_dep + walk_time_to_target
        previous_trip = None
        for trip_tuple in all_pareto_optimal_tuples:
            if previous_trip:
                assert trip_tuple.arrival_time_target > previous_trip.arrival_time_target
            if (
                trip_tuple.departure_time > self.end_time_dep
                and trip_tuple.arrival_time_target < arrival_time_target_at_end_time
            ):
                arrival_time_target_at_end_time = trip_tuple.arrival_time_target
            previous_trip = trip_tuple

        self._walk_time_to_target = walk_time_to_target
        self._profile_blocks = []
        previous_departure_time = start_time_dep
        self.trip_durations = []
        self.trip_departure_times = []
        for trip_pareto_tuple in all_pareto_optimal_tuples:
            if trip_pareto_tuple.departure_time > self.end_time_dep:
                continue
            if self._walk_time_to_target <= trip_pareto_tuple.duration():
                print(self._walk_time_to_target, trip_pareto_tuple.duration())
                assert self._walk_time_to_target > trip_pareto_tuple.duration()
            effective_trip_previous_departure_time = max(
                previous_departure_time,
                trip_pareto_tuple.departure_time
                - (self._walk_time_to_target - trip_pareto_tuple.duration()),
            )
            if effective_trip_previous_departure_time > previous_departure_time:
                walk_block = ProfileBlock(
                    start_time=previous_departure_time,
                    end_time=effective_trip_previous_departure_time,
                    distance_start=self._walk_time_to_target,
                    distance_end=self._walk_time_to_target,
                )
                self._profile_blocks.append(walk_block)
            trip_waiting_time = (
                trip_pareto_tuple.departure_time - effective_trip_previous_departure_time
            )
            trip_block = ProfileBlock(
                end_time=trip_pareto_tuple.departure_time,
                start_time=effective_trip_previous_departure_time,
                distance_start=trip_pareto_tuple.duration() + trip_waiting_time,
                distance_end=trip_pareto_tuple.duration(),
            )
            self.trip_durations.append(trip_pareto_tuple.duration())
            self.trip_departure_times.append(trip_pareto_tuple.departure_time)
            self._profile_blocks.append(trip_block)
            previous_departure_time = trip_pareto_tuple.departure_time

        # deal with last (or add walking block like above)
        if not self._profile_blocks or self._profile_blocks[-1].end_time < end_time_dep:
            if len(self._profile_blocks) > 0:
                dep_previous = self._profile_blocks[-1].end_time
            else:
                dep_previous = start_time_dep
            waiting_time = end_time_dep - dep_previous

            distance_end_trip = arrival_time_target_at_end_time - end_time_dep
            walking_wait_time = min(
                end_time_dep - dep_previous,
                waiting_time - (self._walk_time_to_target - distance_end_trip),
            )
            walking_wait_time = max(0, walking_wait_time)
            if walking_wait_time > 0:
                walk_block = ProfileBlock(
                    start_time=dep_previous,
                    end_time=dep_previous + walking_wait_time,
                    distance_start=self._walk_time_to_target,
                    distance_end=self._walk_time_to_target,
                )
                assert walk_block.start_time <= walk_block.end_time
                assert walk_block.distance_end <= walk_block.distance_start
                self._profile_blocks.append(walk_block)
            trip_waiting_time = waiting_time - walking_wait_time

            if trip_waiting_time > 0:
                try:
                    trip_block = ProfileBlock(
                        start_time=dep_previous + walking_wait_time,
                        end_time=dep_previous + walking_wait_time + trip_waiting_time,
                        distance_start=distance_end_trip + trip_waiting_time,
                        distance_end=distance_end_trip,
                    )
                    assert trip_block.start_time <= trip_block.end_time
                    assert trip_block.distance_end <= trip_block.distance_start
                    self._profile_blocks.append(trip_block)
                except AssertionError as e:
                    # the error was due to a very small waiting timesmall numbers
                    assert trip_waiting_time < 10 ** -5

        # TODO? Refactor to use the cutoff_distance feature in ProfileBlockAnalyzer?
        self.profile_block_analyzer = ProfileBlockAnalyzer(profile_blocks=self._profile_blocks)

    def n_pareto_optimal_trips(self):
        """
        Get number of pareto-optimal trips

        Returns
        -------
        n_trips: float
        """
        return float(len(self.trip_durations))

    @_if_no_trips_return_inf
    def min_trip_duration(self):
        """
        Get minimum travel time to destination.

        Returns
        -------
        float: min_trip_duration
            float('nan') if no trips take place
        """
        return numpy.min(self.trip_durations)

    @_if_no_trips_return_inf
    def max_trip_duration(self):
        """
        Get minimum travel time to destination.

        Returns
        -------
        float: max_trip_duration
            float('inf') if no trips take place
        """
        return numpy.max(self.trip_durations)

    @_if_no_trips_return_inf
    def mean_trip_duration(self):
        """
        Get average travel time to destination.

        Returns
        -------
        float: max_trip_duration
            float('inf') if no trips take place
        """
        return numpy.mean(self.trip_durations)

    @_if_no_trips_return_inf
    def median_trip_duration(self):
        """
        Get average travel time to destination.

        Returns
        -------
        float: max_trip_duration
            float('inf') if no trips take place
        """
        return numpy.median(self.trip_durations)

    def mean_temporal_distance(self):
        """
        Get mean temporal distance (in seconds) to the target.

        Returns
        -------
        mean_temporal_distance : float
        """
        total_width = self.end_time_dep - self.start_time_dep
        total_area = sum([block.area() for block in self._profile_blocks])
        return total_area / total_width

    def median_temporal_distance(self):
        """
        Returns
        -------
        median_temporal_distance : float
        """
        return self.profile_block_analyzer.median()

    def min_temporal_distance(self):
        """
        Compute the minimum temporal distance to target.

        Returns
        -------
        min_temporal_distance: float
        """
        return self.profile_block_analyzer.min()

    def max_temporal_distance(self):
        """
        Compute the maximum temporal distance.

        Returns
        -------
        max_temporal_distance : float
        """
        return self.profile_block_analyzer.max()

    def largest_finite_temporal_distance(self):
        """
        Compute the maximum temporal distance.

        Returns
        -------
        max_temporal_distance : float
        """
        return self.profile_block_analyzer.largest_finite_distance()

    def plot_temporal_distance_cdf(self):
        """
        Plot the temporal distance cumulative density function.

        Returns
        -------
        fig: matplotlib.Figure
        """
        xvalues, cdf = self.profile_block_analyzer._temporal_distance_cdf()
        fig = plt.figure()
        ax = fig.add_subplot(111)
        xvalues = numpy.array(xvalues) / 60.0
        ax.plot(xvalues, cdf, "-k")
        ax.fill_between(xvalues, cdf, color="red", alpha=0.2)
        ax.set_ylabel("CDF(t)")
        ax.set_xlabel("Temporal distance t (min)")
        return fig

    def plot_temporal_distance_pdf(self, use_minutes=True, color="green", ax=None):
        """
        Plot the temporal distance probability density function.

        Returns
        -------
        fig: matplotlib.Figure
        """
        from matplotlib import pyplot as plt

        plt.rc("text", usetex=True)
        (
            temporal_distance_split_points_ordered,
            densities,
            delta_peaks,
        ) = self._temporal_distance_pdf()
        xs = []
        for i, x in enumerate(temporal_distance_split_points_ordered):
            xs.append(x)
            xs.append(x)
        xs = numpy.array(xs)
        ys = [0]
        for y in densities:
            ys.append(y)
            ys.append(y)
        ys.append(0)
        ys = numpy.array(ys)
        # convert data to minutes:
        xlabel = "Temporal distance (s)"
        ylabel = "Probability density (t)"
        if use_minutes:
            xs /= 60.0
            ys *= 60.0
            xlabel = "Temporal distance (min)"
            delta_peaks = {peak / 60.0: mass for peak, mass in delta_peaks.items()}

        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot(111)
        ax.plot(xs, ys, "k-")
        ax.fill_between(xs, ys, color="green", alpha=0.2)

        if delta_peaks:
            peak_height = max(ys) * 1.4
            max_x = max(xs)
            min_x = min(xs)
            now_max_x = max(xs) + 0.3 * (max_x - min_x)
            now_min_x = min_x - 0.1 * (max_x - min_x)

            text_x_offset = 0.1 * (now_max_x - max_x)

            for loc, mass in delta_peaks.items():
                ax.plot([loc, loc], [0, peak_height], color="green", lw=5)
                ax.text(
                    loc + text_x_offset,
                    peak_height * 0.99,
                    "$P(\\mathrm{walk}) = %.2f$" % (mass),
                    color="green",
                )
            ax.set_xlim(now_min_x, now_max_x)

            tot_delta_peak_mass = sum(delta_peaks.values())
            transit_text_x = (min_x + max_x) / 2
            transit_text_y = min(ys[ys > 0]) / 2.0
            ax.text(
                transit_text_x,
                transit_text_y,
                "$P(mathrm{PT}) = %.2f$" % (1 - tot_delta_peak_mass),
                color="green",
                va="center",
                ha="center",
            )

        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_ylim(bottom=0)
        return ax.figure

    def plot_temporal_distance_pdf_horizontal(
        self,
        use_minutes=True,
        color="green",
        ax=None,
        duration_divider=60.0,
        legend_font_size=None,
        legend_loc=None,
    ):
        """
        Plot the temporal distance probability density function.

        Returns
        -------
        fig: matplotlib.Figure
        """
        from matplotlib import pyplot as plt

        plt.rc("text", usetex=True)

        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot(111)

        (
            temporal_distance_split_points_ordered,
            densities,
            delta_peaks,
        ) = self._temporal_distance_pdf()
        xs = []
        for i, x in enumerate(temporal_distance_split_points_ordered):
            xs.append(x)
            xs.append(x)
        xs = numpy.array(xs)
        ys = [0]
        for y in densities:
            ys.append(y)
            ys.append(y)
        ys.append(0)
        ys = numpy.array(ys)
        # convert data to minutes:
        xlabel = "Temporal distance (s)"
        ylabel = "Probability density $P(\\tau)$"
        if use_minutes:
            xs /= duration_divider
            ys *= duration_divider
            xlabel = "Temporal distance (min)"
            delta_peaks = {peak / 60.0: mass for peak, mass in delta_peaks.items()}

        if delta_peaks:
            peak_height = max(ys) * 1.4
            max_x = max(xs)
            min_x = min(xs)
            now_max_x = max(xs) + 0.3 * (max_x - min_x)
            now_min_x = min_x - 0.1 * (max_x - min_x)

            text_x_offset = 0.1 * (now_max_x - max_x)

            for loc, mass in delta_peaks.items():
                text = "$P(\\mathrm{walk}) = " + ("%.2f$" % (mass))
                ax.plot([0, peak_height], [loc, loc], color=color, lw=5, label=text)

        ax.plot(ys, xs, "k-")
        if delta_peaks:
            tot_delta_peak_mass = sum(delta_peaks.values())
            fill_label = "$P(\\mathrm{PT}) = %.2f$" % (1 - tot_delta_peak_mass)
        else:
            fill_label = None
        ax.fill_betweenx(xs, ys, color=color, alpha=0.2, label=fill_label)

        ax.set_ylabel(xlabel)
        ax.set_xlabel(ylabel)
        ax.set_xlim(left=0, right=max(ys) * 1.2)
        if delta_peaks:
            if legend_font_size is None:
                legend_font_size = 12
            if legend_loc is None:
                legend_loc = "best"
            ax.legend(loc=legend_loc, prop={"size": legend_font_size})

        if True:
            line_tyles = ["-.", "--", "-"][::-1]
            to_plot_funcs = [
                self.max_temporal_distance,
                self.mean_temporal_distance,
                self.min_temporal_distance,
            ]

            xmin, xmax = ax.get_xlim()
            for to_plot_func, ls in zip(to_plot_funcs, line_tyles):
                y = to_plot_func() / duration_divider
                assert y < float("inf")
                # factor of 10 just to be safe that the lines cover the whole region.
                ax.plot([xmin, xmax * 10], [y, y], color="black", ls=ls, lw=1)

        return ax.figure

    def plot_temporal_distance_profile(
        self,
        timezone=None,
        color="black",
        alpha=0.15,
        ax=None,
        lw=2,
        label="",
        plot_tdist_stats=False,
        plot_trip_stats=False,
        format_string="%Y-%m-%d %H:%M:%S",
        plot_journeys=False,
        duration_divider=60.0,
        fill_color="green",
        journey_letters=None,
        return_letters=False,
    ):
        """
        Parameters
        ----------
        timezone: str
        color: color
        format_string: str, None
            if None, the original values are used
        plot_journeys: bool, optional
            if True, small dots are plotted at the departure times
        """
        if ax is None:
            fig = plt.figure()
            ax = fig.add_subplot(111)

        if timezone is None:
            warnings.warn("Warning: No timezone specified, defaulting to UTC")
            timezone = pytz.timezone("Etc/UTC")

        def _ut_to_unloc_datetime(ut):
            dt = datetime.datetime.fromtimestamp(ut, timezone)
            return dt.replace(tzinfo=None)

        if format_string:
            x_axis_formatter = md.DateFormatter(format_string)
            ax.xaxis.set_major_formatter(x_axis_formatter)
        else:
            _ut_to_unloc_datetime = lambda x: x

        ax.set_xlim(
            _ut_to_unloc_datetime(self.start_time_dep), _ut_to_unloc_datetime(self.end_time_dep)
        )

        if plot_tdist_stats:
            line_tyles = ["-.", "--", "-"][::-1]
            # to_plot_labels = ["maximum temporal distance", "mean temporal distance", "minimum temporal distance"]
            to_plot_labels = [
                "$\\tau_\\mathrm{max} \\;$ = ",
                "$\\tau_\\mathrm{mean}$ = ",
                "$\\tau_\\mathrm{min} \\:\\:$ = ",
            ]
            to_plot_funcs = [
                self.max_temporal_distance,
                self.mean_temporal_distance,
                self.min_temporal_distance,
            ]

            xmin, xmax = ax.get_xlim()
            for to_plot_label, to_plot_func, ls in zip(to_plot_labels, to_plot_funcs, line_tyles):
                y = to_plot_func() / duration_divider
                assert y < float("inf"), to_plot_label
                to_plot_label = to_plot_label + "%.1f min" % (y)
                ax.plot([xmin, xmax], [y, y], color="black", ls=ls, lw=1, label=to_plot_label)

        if plot_trip_stats:
            assert not plot_tdist_stats
            line_tyles = ["-", "-.", "--"]
            to_plot_labels = [
                "min journey duration",
                "max journey duration",
                "mean journey duration",
            ]
            to_plot_funcs = [
                self.min_trip_duration,
                self.max_trip_duration,
                self.mean_trip_duration,
            ]

            xmin, xmax = ax.get_xlim()
            for to_plot_label, to_plot_func, ls in zip(to_plot_labels, to_plot_funcs, line_tyles):
                y = to_plot_func() / duration_divider
                if not numpy.math.isnan(y):
                    ax.plot([xmin, xmax], [y, y], color="red", ls=ls, lw=2)
                    txt = to_plot_label + "\n = %.1f min" % y
                    ax.text(
                        xmax + 0.01 * (xmax - xmin), y, txt, color="red", va="center", ha="left"
                    )

            old_xmax = xmax
            xmax += (xmax - xmin) * 0.3
            ymin, ymax = ax.get_ylim()
            ax.fill_between([old_xmax, xmax], ymin, ymax, color="gray", alpha=0.1)
            ax.set_xlim(xmin, xmax)

        # plot the actual profile
        vertical_lines, slopes = self.profile_block_analyzer.get_vlines_and_slopes_for_plotting()
        for i, line in enumerate(slopes):
            xs = [_ut_to_unloc_datetime(x) for x in line["x"]]
            if i is 0:
                label = "profile"
            else:
                label = None
            ax.plot(
                xs, numpy.array(line["y"]) / duration_divider, "-", color=color, lw=lw, label=label
            )

        for line in vertical_lines:
            xs = [_ut_to_unloc_datetime(x) for x in line["x"]]
            ax.plot(xs, numpy.array(line["y"]) / duration_divider, ":", color=color)  # , lw=lw)

        assert isinstance(ax, plt.Axes)

        if plot_journeys:
            xs = [_ut_to_unloc_datetime(x) for x in self.trip_departure_times]
            ys = self.trip_durations
            ax.plot(
                xs, numpy.array(ys) / duration_divider, "o", color="black", ms=8, label="journeys"
            )
            if journey_letters is None:
                journey_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

                def cycle_journey_letters(journey_letters):
                    # cycle('ABCD') --> A B C D A B C D A B C D ...
                    saved = []
                    for element in journey_letters:
                        yield element
                        saved.append(element)
                    count = 1
                    while saved:
                        for element in saved:
                            yield element + str(count)
                        count += 1

                journey_letters_iterator = cycle_journey_letters(journey_letters)
            time_letters = {
                int(time): letter
                for letter, time in zip(journey_letters_iterator, self.trip_departure_times)
            }
            for x, y, letter in zip(xs, ys, journey_letters_iterator):
                walking = (
                    -self._walk_time_to_target / 30
                    if numpy.isfinite(self._walk_time_to_target)
                    else 0
                )
                ax.text(
                    x + datetime.timedelta(seconds=(self.end_time_dep - self.start_time_dep) / 60),
                    (y + walking) / duration_divider,
                    letter,
                    va="top",
                    ha="left",
                )

        fill_between_x = []
        fill_between_y = []
        for line in slopes:
            xs = [_ut_to_unloc_datetime(x) for x in line["x"]]
            fill_between_x.extend(xs)
            fill_between_y.extend(numpy.array(line["y"]) / duration_divider)

        ax.fill_between(
            fill_between_x, y1=fill_between_y, color=fill_color, alpha=alpha, label=label
        )

        ax.set_ylim(bottom=0)
        ax.set_ylim(ax.get_ylim()[0], ax.get_ylim()[1] * 1.05)

        if rcParams["text.usetex"]:
            ax.set_xlabel(r"Departure time $t_{\mathrm{dep}}$")
        else:
            ax.set_xlabel("Departure time")

        ax.set_ylabel(r"Temporal distance $\tau$ (min)")
        if plot_journeys and return_letters:
            return ax, time_letters
        else:
            return ax

    def _temporal_distance_pdf(self):
        """
        Temporal distance probability density function.

        Returns
        -------
        non_delta_peak_split_points: numpy.array
        non_delta_peak_densities: numpy.array
            len(density) == len(temporal_distance_split_points_ordered) -1
        delta_peak_loc_to_probability_mass : dict
        """
        (
            temporal_distance_split_points_ordered,
            norm_cdf,
        ) = self.profile_block_analyzer._temporal_distance_cdf()
        delta_peak_loc_to_probability_mass = {}

        non_delta_peak_split_points = [temporal_distance_split_points_ordered[0]]
        non_delta_peak_densities = []
        for i in range(0, len(temporal_distance_split_points_ordered) - 1):
            left = temporal_distance_split_points_ordered[i]
            right = temporal_distance_split_points_ordered[i + 1]
            width = right - left
            prob_mass = norm_cdf[i + 1] - norm_cdf[i]
            if width == 0.0:
                delta_peak_loc_to_probability_mass[left] = prob_mass
            else:
                non_delta_peak_split_points.append(right)
                non_delta_peak_densities.append(prob_mass / float(width))
        assert len(non_delta_peak_densities) == len(non_delta_peak_split_points) - 1
        return (
            numpy.array(non_delta_peak_split_points),
            numpy.array(non_delta_peak_densities),
            delta_peak_loc_to_probability_mass,
        )

    def get_temporal_distance_at(self, dep_time):
        return self.profile_block_analyzer.interpolate(dep_time)

    @staticmethod
    def all_measures_and_names_as_lists():
        NPA = NodeProfileAnalyzerTime
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
        ]
        assert len(profile_summary_methods) == len(profile_observable_names)
        return profile_summary_methods, profile_observable_names
