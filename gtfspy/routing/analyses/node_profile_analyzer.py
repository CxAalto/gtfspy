import datetime
import numpy
import pytz

from matplotlib import dates as md
import matplotlib.pyplot as plt

from gtfspy.routing.models import ParetoTuple

class NodeProfileAnalyzer:

    def __init__(self, node_profile, start_time_dep, end_time_dep):
        """
        Initialize the data structures required by

        Parameters
        ----------
        node_profile: NodeProfile
        """
        self.start_time_dep = start_time_dep
        self.end_time_dep = end_time_dep

        # used for computing temporal distances:
        pareto_tuples = [pt for pt in node_profile.get_pareto_tuples() if
                         (start_time_dep <= pt.departure_time <= end_time_dep)]
        pareto_tuples = sorted(pareto_tuples, key=lambda pt: pt.departure_time)
        pareto_tuples.append(
            ParetoTuple(departure_time=end_time_dep,
                        arrival_time_target=node_profile.get_earliest_arrival_time_at_target(end_time_dep))
        )
        _virtual_waiting_times = []
        _virtual_durations = []
        _virtual_dep_times = []
        _virtual_arrival_times = []
        previous_departure_time = start_time_dep
        for pareto_tuple in pareto_tuples:
            _virtual_waiting_times.append(pareto_tuple.departure_time - previous_departure_time)
            _virtual_durations.append(pareto_tuple.duration())
            _virtual_dep_times.append(pareto_tuple.departure_time)
            _virtual_arrival_times.append(pareto_tuple.arrival_time_target)
            previous_departure_time = pareto_tuple.departure_time
        self._virtual_dep_times = numpy.array(_virtual_dep_times)
        self._virtual_durations = numpy.array(_virtual_durations)
        self._virtual_waiting_times = numpy.array(_virtual_waiting_times)
        self._virtual_arrival_times = numpy.array(_virtual_arrival_times)
        self.trip_durations = self._virtual_durations[:-1]


    def min_trip_duration(self):
        """
        Get minimum travel time to destination.

        Returns
        -------
        float: min_trip_duration
            None if no trips take place
        """
        if len(self.trip_durations) is 0:
            return None
        else:
            return numpy.min(self.trip_durations)

    def max_trip_duration(self):
        """
        Get minimum travel time to destination.

        Returns
        -------
        float: max_trip_duration
            None if no trips take place
        """
        if len(self.trip_durations) is 0:
            return None
        else:
            return numpy.max(self.trip_durations)

    def mean_trip_duration(self):
        """
        Get average travel time to destination.

        Returns
        -------
        float: max_trip_duration
            None if no trips take place
        """
        if len(self.trip_durations) == 0:
            return None
        else:
            return numpy.mean(self.trip_durations)

    def median_trip_duration(self):
        """
        Get average travel time to destination.

        Returns
        -------
        float: max_trip_duration
            None if no trips take place
        """
        if len(self.trip_durations) is 0:
            return None
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
        total_area = 0
        for waiting_time, duration in zip(self._virtual_waiting_times, self._virtual_durations):
            total_area += duration * waiting_time + (waiting_time * waiting_time) / 2.
        return total_area / total_width

    def median_temporal_distance(self):
        """
        Returns
        -------
        median_temporal_distance : float
        """
        temporal_distance_split_points = set()
        for waiting_time, duration in zip(self._virtual_waiting_times, self._virtual_durations):
            temporal_distance_split_points.add(duration)
            temporal_distance_split_points.add(duration + waiting_time)
        temporal_distance_split_points_ordered = numpy.array(sorted(list(temporal_distance_split_points)))
        temporal_distance_split_widths = temporal_distance_split_points_ordered[1:] - \
            temporal_distance_split_points_ordered[:-1]

        counts = numpy.zeros(len(temporal_distance_split_widths))
        for waiting_time, duration in zip(self._virtual_waiting_times, self._virtual_durations):
            start_index = numpy.searchsorted(temporal_distance_split_points_ordered, duration)
            end_index = numpy.searchsorted(temporal_distance_split_points_ordered, duration + waiting_time)
            counts[start_index:end_index] += 1
        unnorm_cdf = numpy.array([0] + list(numpy.cumsum(temporal_distance_split_widths * counts)))
        assert (unnorm_cdf[-1] == self.end_time_dep - self.start_time_dep)
        norm_cdf = unnorm_cdf / unnorm_cdf[-1]
        left = numpy.searchsorted(norm_cdf, 0.5, side="right")
        right = numpy.searchsorted(norm_cdf, 0.5, side="left")
        if left == right:
            left_cdf_val = norm_cdf[left - 1]
            right_cdf_val = norm_cdf[left]
            delta_y = right_cdf_val - left_cdf_val
            assert (delta_y > 0)
            delta_x = (temporal_distance_split_points_ordered[left] - temporal_distance_split_points_ordered[left -1])
            median = (0.5 - left_cdf_val) / delta_y * delta_x + temporal_distance_split_points_ordered[left - 1]
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
        return min(self._virtual_durations)

    def max_temporal_distance(self):
        """
        Compute the maximum temporal distance.

        Returns
        -------
        max_temporal_distance : float
        """
        return (numpy.array(self._virtual_durations) + numpy.array(self._virtual_waiting_times)).max()

    def plot_cdf(self):
        # use
        pass

    def plot_pdf(self):
        # use
        pass

    def plot_temporal_variation(self, timezone=None, show=False):
        """
        See plots.py: plot_temporal_distance_variation for more documentation.
        """
        fig = plt.figure()
        ax = fig.add_subplot(111)

        vlines = []
        slopes = []

        for i, (departure_time, duration, waiting_time) in enumerate(zip(
                self._virtual_dep_times,
                self._virtual_durations,
                self._virtual_waiting_times)):
            if i == len(self._virtual_dep_times) - 1:
                continue  # do not do anything for the last as it is virtual

            if i == 0:
                previous_dep_time = self.start_time_dep
            else:
                previous_dep_time = self._virtual_dep_times[i - 1]


            duration_minutes = duration / 60.0
            waiting_time_minutes = waiting_time / 60.

            duration_after_previous_departure_minutes = duration_minutes + waiting_time_minutes

            slope = dict(x=[previous_dep_time, departure_time],
                         y=[duration_after_previous_departure_minutes, duration_minutes])
            slopes.append(slope)

            if i != 0:
                previous_duration_minutes = self._virtual_durations[i -1] / 60.0
                vlines.append(dict(x=[previous_dep_time, previous_dep_time],
                                   y=[previous_duration_minutes, duration_after_previous_departure_minutes]))

        if timezone is None:
            print("Warning: No timezone specified, defaulting to UTC")
            timezone = pytz.timezone("Etc/UTC")

        def _ut_to_unloc_datetime(ut):
            dt = datetime.datetime.fromtimestamp(ut, timezone)
            return dt.replace(tzinfo=None)

        format_string = "%Y-%m-%d %H:%M:%S"
        x_axis_formatter = md.DateFormatter(format_string)
        ax.xaxis.set_major_formatter(x_axis_formatter)

        for line in slopes:
            xs = [_ut_to_unloc_datetime(x) for x in line['x']]
            ax.plot(xs, line['y'], "-", color="black")
        for line in vlines:
            xs = [_ut_to_unloc_datetime(x) for x in line['x']]
            ax.plot(xs, line['y'], "--", color="black")

        assert (isinstance(ax, plt.Axes))

        fill_between_x = []
        fill_between_y = []
        for line in slopes:
            xs = [_ut_to_unloc_datetime(x) for x in line['x']]
            fill_between_x.extend(xs)
            fill_between_y.extend(line["y"])

        ax.fill_between(fill_between_x, y1=fill_between_y, color="red", alpha=0.2)

        ax.set_ylim(bottom=0)
        ax.set_xlim(
            _ut_to_unloc_datetime(self.start_time_dep),
            _ut_to_unloc_datetime(self.end_time_dep)
        )
        ax.set_xlabel("Departure time")
        ax.set_ylabel("Duration to destination (min)")
        fig.tight_layout()
        plt.xticks(rotation=45)
        plt.subplots_adjust(bottom=0.3)
        if show:
            plt.show()
        return fig
