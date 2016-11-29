import datetime
import warnings
from collections import namedtuple

import numpy
import pytz

from matplotlib import dates as md
import matplotlib.pyplot as plt

from gtfspy.routing.node_profile_simple import NodeProfileSimple

_profile_block = namedtuple('ProfileBlock',
                          ['departure_time',
                           'waiting_time',
                           'distance_start',
                           'distance_end',
                           'is_public_transport_trip_within_range'])


class _ProfileBlock(_profile_block):

    def area(self):
        return self.waiting_time * 0.5 * (self.distance_start + self.distance_end)


class NodeProfileAnalyzerTime:

    def __init__(self, node_profile, start_time_dep, end_time_dep):
        """
        Initialize the data structures required by

        Parameters
        ----------
        node_profile: NodeProfileSimple
        """
        self.start_time_dep = start_time_dep
        self.end_time_dep = end_time_dep
        assert(isinstance(node_profile, NodeProfileSimple))
        # used for computing temporal distances:
        trip_pareto_optimal_tuples = [pt for pt in node_profile.get_final_optimal_labels() if
                                      (start_time_dep < pt.departure_time <= end_time_dep)]
        trip_pareto_optimal_tuples = sorted(trip_pareto_optimal_tuples, key=lambda ptuple: ptuple.departure_time)
        self._walk_time_to_target = node_profile.get_walk_to_target_duration()
        self._profile_blocks = []
        previous_departure_time = start_time_dep
        for trip_pareto_tuple in trip_pareto_optimal_tuples:
            effective_trip_previous_departure_time = max(
                previous_departure_time,
                trip_pareto_tuple.departure_time - (self._walk_time_to_target - trip_pareto_tuple.duration())
            )
            walk_waiting_time = effective_trip_previous_departure_time - previous_departure_time
            if walk_waiting_time > 0:
                walk_block = _ProfileBlock(departure_time=effective_trip_previous_departure_time,
                                           waiting_time=walk_waiting_time,
                                           distance_start=self._walk_time_to_target,
                                           distance_end=self._walk_time_to_target,
                                           is_public_transport_trip_within_range=False
                                           )
                self._profile_blocks.append(walk_block)
            trip_waiting_time = trip_pareto_tuple.departure_time - effective_trip_previous_departure_time
            trip_block = _ProfileBlock(departure_time=trip_pareto_tuple.departure_time,
                                       waiting_time=trip_waiting_time,
                                       distance_start=trip_pareto_tuple.duration() + trip_waiting_time,
                                       distance_end=trip_pareto_tuple.duration(),
                                       is_public_transport_trip_within_range=True
                                       )
            self._profile_blocks.append(trip_block)
            previous_departure_time = trip_pareto_tuple.departure_time

        # deal with last (or add walking block like above)
        if not self._profile_blocks or self._profile_blocks[-1].departure_time < end_time_dep:
            if len(self._profile_blocks) > 0:
                dep_previous = self._profile_blocks[-1].departure_time
            else:
                dep_previous = start_time_dep
            waiting_time = end_time_dep - dep_previous
            arrival_time_target_at_end_time = node_profile.evaluate_earliest_arrival_time_at_target(end_time_dep, 0)
            distance_end_trip = arrival_time_target_at_end_time - end_time_dep
            walking_wait_time = min(end_time_dep - dep_previous,
                                    waiting_time - (self._walk_time_to_target - distance_end_trip))
            walking_wait_time = max(0, walking_wait_time)
            if walking_wait_time > 0:
                walk_block = _ProfileBlock(departure_time=dep_previous,
                                           waiting_time=walking_wait_time,
                                           distance_start=self._walk_time_to_target,
                                           distance_end=self._walk_time_to_target,
                                           is_public_transport_trip_within_range=False
                                           )
                self._profile_blocks.append(walk_block)
            trip_waiting_time = waiting_time - walking_wait_time

            if trip_waiting_time > 0:
                trip_block = _ProfileBlock(departure_time=dep_previous + walking_wait_time,
                                           waiting_time=trip_waiting_time,
                                           distance_start=distance_end_trip + trip_waiting_time,
                                           distance_end=distance_end_trip,
                                           is_public_transport_trip_within_range=False
                                           )
                self._profile_blocks.append(trip_block)
        self.trip_durations = [block.distance_end for block in self._profile_blocks
                               if block.is_public_transport_trip_within_range]

    def n_pareto_optimal_trips(self):
        """
        Get number of pareto-optimal trips

        Returns
        -------
        n_trips: float
        """
        return float(len(self.trip_durations))

    def min_trip_duration(self):
        """
        Get minimum travel time to destination.

        Returns
        -------
        float: min_trip_duration
            float('inf') if no trips take place
        """
        if len(self.trip_durations) is 0:
            return float('inf')
        else:
            return numpy.min(self.trip_durations)

    def max_trip_duration(self):
        """
        Get minimum travel time to destination.

        Returns
        -------
        float: max_trip_duration
            float('inf') if no trips take place
        """
        if len(self.trip_durations) is 0:
            return float('inf')
        else:
            return numpy.max(self.trip_durations)

    def mean_trip_duration(self):
        """
        Get average travel time to destination.

        Returns
        -------
        float: max_trip_duration
            float('inf') if no trips take place
        """
        if len(self.trip_durations) == 0:
            return float('inf')
        else:
            return numpy.mean(self.trip_durations)

    def median_trip_duration(self):
        """
        Get average travel time to destination.

        Returns
        -------
        float: max_trip_duration
            float('inf') if no trips take place
        """
        if len(self.trip_durations) is 0:
            return float('inf')
        else:
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
        temporal_distance_split_points_ordered, norm_cdf = self._temporal_distance_cdf()
        if len(temporal_distance_split_points_ordered) == 0:
            return float('inf')

        left = numpy.searchsorted(norm_cdf, 0.5, side="left")
        right = numpy.searchsorted(norm_cdf, 0.5, side="right")
        if left == len(norm_cdf):
            return float('inf')
        elif left == right:
            left_cdf_val = norm_cdf[right - 1]
            right_cdf_val = norm_cdf[right]
            delta_y = right_cdf_val - left_cdf_val
            assert (delta_y > 0)
            delta_x = (temporal_distance_split_points_ordered[right] - temporal_distance_split_points_ordered[right - 1])
            median = (0.5 - left_cdf_val) / delta_y * delta_x + temporal_distance_split_points_ordered[right - 1]
            return median
        else:
            return temporal_distance_split_points_ordered[left]

    def min_temporal_distance(self):
        """
        Compute the minimum temporal distance to target.

        Returns
        -------
        min_temporal_distance: float
        """
        return min([block.distance_end for block in self._profile_blocks])

    def max_temporal_distance(self):
        """
        Compute the maximum temporal distance.

        Returns
        -------
        max_temporal_distance : float
        """
        block_start_durations = [block.distance_start for block in self._profile_blocks]
        return max(block_start_durations)

    def largest_finite_temporal_distance(self):
        """
        Compute the maximum temporal distance.

        Returns
        -------
        max_temporal_distance : float
        """
        block_start_durations = [block.distance_start for block in self._profile_blocks if block.distance_start < float('inf')]
        if block_start_durations:
            return max(block_start_durations)
        else:
            return None

    def plot_temporal_distance_cdf(self):
        """
        Plot the temporal distance cumulative density function.

        Returns
        -------
        fig: matplotlib.Figure
        """
        xvalues, cdf = self._temporal_distance_cdf()
        fig = plt.figure()
        ax = fig.add_subplot(111)
        xvalues = numpy.array(xvalues) / 60.0
        ax.plot(xvalues, cdf, "-k")
        ax.fill_between(xvalues, cdf, color="red", alpha=0.2)
        ax.set_ylabel("CDF(t)")
        ax.set_xlabel("Temporal distance t (min)")
        return fig

    def plot_temporal_distance_pdf(self):
        """
        Plot the temporal distance probability density function.

        Returns
        -------
        fig: matplotlib.Figure
        """
        temporal_distance_split_points_ordered, densities = self._temporal_distance_pdf()
        xs = []
        for i, x in enumerate(temporal_distance_split_points_ordered):
            xs.append(x)
            xs.append(x)
        ys = [0]
        for y in densities:
            ys.append(y)
            ys.append(y)
        ys.append(0)
        # convert data to minutes:
        xs = numpy.array(xs) / 60.0
        ys = numpy.array(ys) * 60.0

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(xs, ys, "k-")
        ax.fill_between(xs, ys, color="red", alpha=0.2)
        ax.set_xlabel("Temporal distance t (min)")
        ax.set_ylabel("PDF(t) t (min)")
        ax.set_ylim(bottom=0)
        return fig

    def _get_vlines_and_slopes_temporal_distance_variation(self):
        vertical_lines = []
        slopes = []

        for i, block in enumerate(self._profile_blocks):
            previous_dep_time = block.departure_time - block.waiting_time

            distance_end_minutes = block.distance_end / 60.0
            distance_start_minutes = block.distance_start / 60.0

            slope = dict(x=[previous_dep_time, block.departure_time],
                         y=[distance_start_minutes, distance_end_minutes])
            slopes.append(slope)

            if i != 0:
                # no vertical line for the first observation
                previous_duration_minutes = self._profile_blocks[i - 1].distance_end / 60.0
                vertical_lines.append(dict(x=[previous_dep_time, previous_dep_time],
                                           y=[previous_duration_minutes, distance_start_minutes]))
        return vertical_lines, slopes

    def plot_temporal_distance_variation(self, timezone=None, color="red", alpha=0.15, ax=None, lw=None, label=""):
        """
        See plots.py: plot_temporal_distance_variation for more documentation.
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

        format_string = "%Y-%m-%d %H:%M:%S"
        x_axis_formatter = md.DateFormatter(format_string)
        ax.xaxis.set_major_formatter(x_axis_formatter)

        vertical_lines, slopes = self._get_vlines_and_slopes_temporal_distance_variation()
        for i, line in enumerate(slopes):
            xs = [_ut_to_unloc_datetime(x) for x in line['x']]
            ax.plot(xs, line['y'], "-", color="black", lw=lw)
        for line in vertical_lines:
            xs = [_ut_to_unloc_datetime(x) for x in line['x']]
            ax.plot(xs, line['y'], "--", color="black") #, lw=lw)

        assert (isinstance(ax, plt.Axes))

        fill_between_x = []
        fill_between_y = []
        for line in slopes:
            xs = [_ut_to_unloc_datetime(x) for x in line['x']]
            fill_between_x.extend(xs)
            fill_between_y.extend(line["y"])

        ax.fill_between(fill_between_x, y1=fill_between_y, color=color, alpha=alpha, label=label)

        ax.set_ylim(bottom=0)
        ax.set_xlim(
            _ut_to_unloc_datetime(self.start_time_dep),
            _ut_to_unloc_datetime(self.end_time_dep)
        )
        ax.set_xlabel("Departure time")
        ax.set_ylabel("Duration to destination (min)")
        ax.figure.tight_layout()
        plt.xticks(rotation=45)
        ax.figure.subplots_adjust(bottom=0.3)
        return ax.figure

    def _temporal_distance_cdf(self):
        """
        Temporal distance cumulative density function.

        Returns
        -------
        x_values: numpy.array
            values for the x-axis
        cdf: numpy.array
            cdf values
        """
        temporal_distance_split_points = set()
        for block in self._profile_blocks:
            if block.distance_start != float('inf'):
                temporal_distance_split_points.add(block.distance_end)
                temporal_distance_split_points.add(block.distance_start)

        temporal_distance_split_points_ordered = numpy.array(sorted(list(temporal_distance_split_points)))
        temporal_distance_split_widths = temporal_distance_split_points_ordered[1:] - \
                                         temporal_distance_split_points_ordered[:-1]
        walk_total_time = 0
        infinity_waiting_time = 0
        trip_counts = numpy.zeros(len(temporal_distance_split_widths))

        for block in self._profile_blocks:
            if block.distance_start == float('inf'):
                infinity_waiting_time += block.waiting_time
            elif block.distance_start == block.distance_end == self._walk_time_to_target:
                walk_total_time += block.waiting_time
            else:
                start_index = numpy.searchsorted(temporal_distance_split_points_ordered, block.distance_end)
                end_index = numpy.searchsorted(temporal_distance_split_points_ordered, block.distance_start)
                trip_counts[start_index:end_index] += 1

        unnormalized_cdf = numpy.array([0] + list(numpy.cumsum(temporal_distance_split_widths * trip_counts)))
        assert(numpy.isclose(
                [unnormalized_cdf[-1]],
                [self.end_time_dep - self.start_time_dep - infinity_waiting_time - walk_total_time]
            ).all())

        if walk_total_time > 0:
            index = numpy.nonzero(temporal_distance_split_points_ordered == self._walk_time_to_target)[0][0]
            unnormalized_cdf = numpy.insert(unnormalized_cdf, index, unnormalized_cdf[index])
            temporal_distance_split_points_ordered = numpy.insert(temporal_distance_split_points_ordered, index, temporal_distance_split_points_ordered[index])
            # walk_waiting_time_fraction = walk_total_time / (self.end_time_dep - self.start_time_dep)
            unnormalized_cdf[(index + 1):] = unnormalized_cdf[(index + 1):] + walk_total_time # walk_waiting_time_fraction

        norm_cdf = unnormalized_cdf / (unnormalized_cdf[-1] + infinity_waiting_time)
        return temporal_distance_split_points_ordered, norm_cdf

    def _temporal_distance_pdf(self):
        """
        Temporal distance probability density function.

        Returns
        -------
        temporal_distance_split_points_ordered: numpy.array
        density: numpy.array
            len(density) == len(temporal_distance_split_points_ordered) -1
        """
        raise NotImplementedError("One should figure out, how to deal with delta functions due to walk times")
        # temporal_distance_split_points_ordered, norm_cdf = self._temporal_distance_cdf()
        # temporal_distance_split_widths = temporal_distance_split_points_ordered[1:] - \
        #                                  temporal_distance_split_points_ordered[:-1]
        # densities = (norm_cdf[1:] - norm_cdf[:-1]) / temporal_distance_split_widths
        # assert (len(densities) == len(temporal_distance_split_points_ordered) - 1)
        # return temporal_distance_split_points_ordered, densities

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
            NPA.n_pareto_optimal_trips
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
            "n_pareto_optimal_trips"
        ]
        assert(len(profile_summary_methods) == len(profile_observable_names))
        return profile_summary_methods, profile_observable_names
