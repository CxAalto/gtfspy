from __future__ import print_function

import numpy
import matplotlib.pyplot as plt

from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTimeAndVehLegCount, compute_pareto_front, LabelTimeSimple
from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime
from gtfspy.routing.node_profile_simple import NodeProfileSimple


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


class NodeProfileAnalyzerTimeAndVehLegs:

    def __init__(self, node_profile, start_time_dep, end_time_dep):
        """
        Initialize the data structures required by

        Parameters
        ----------
        node_profile: NodeProfileMultiObjective
        """
        self.node_profile = node_profile
        assert(self.node_profile.label_class == LabelTimeAndVehLegCount)
        self.start_time_dep = start_time_dep
        self.end_time_dep = end_time_dep
        self.all_labels = [label for label in node_profile.get_final_optimal_labels() if
                           (start_time_dep <= label.departure_time < end_time_dep)]
        self.all_labels.extend(self.node_profile.evaluate_at_arbitrary_time(end_time_dep, allow_walk_to_target=False))
        self._labels_within_time_frame = self.all_labels[::-1]

        self._walk_to_target_duration = self.node_profile.get_walk_to_target_duration()
        self._n_veh_legs_to_simple_time_analyzers = {}

    @_check_for_no_labels_for_n_veh_counts
    def max_trip_n_boardings(self):
        return numpy.max([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_check_for_no_labels_for_n_veh_counts
    def min_trip_n_boardings(self):
        return numpy.min([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_check_for_no_labels_for_n_veh_counts
    def mean_trip_n_boardings(self):
        return numpy.mean([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_check_for_no_labels_for_n_veh_counts
    def median_trip_n_boardings(self):
        return numpy.median([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_check_for_no_labels_for_n_veh_counts
    def temporal_mean_n_boardings(self):


        return numpy.median([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_if_no_labels_return_inf
    def min_temporal_distance(self):
        return self._get_time_profile_analyzer().min_temporal_distance()

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

    def median_temporal_distances(self, min_n_veh_legs=None, max_n_veh_legs=None):
        """
        Returns
        -------
        mean_temporal_distances: list
            list indices encode the number of vehicle legs each element
            in the list tells gets the mean temporal distance
        """
        if min_n_veh_legs is None:
            min_n_veh_legs = 0

        if max_n_veh_legs is None:
            max_n_veh_legs = self.max_trip_n_boardings()
            if max_n_veh_legs is None:
                max_n_veh_legs = 0

        median_temporal_distances = [float('inf') for _ in range(min_n_veh_legs, max_n_veh_legs + 1)]
        for n_veh_legs in range(min_n_veh_legs, max_n_veh_legs + 1):
            simple_analyzer = self._get_time_profile_analyzer(n_veh_legs)
            median_temporal_distances[n_veh_legs] = simple_analyzer.median_temporal_distance()
        return median_temporal_distances

    def _get_time_profile_analyzer(self, n_vehicle_legs=None):
        """
        Parameters
        ----------
        n_vehicle_legs: int

        Returns
        -------
        analyzer: NodeProfileAnalyzerTime
        """
        if n_vehicle_legs is None:
            n_vehicle_legs = self.max_trip_n_boardings()
        # compute only if not yet computed
        if not n_vehicle_legs in self._n_veh_legs_to_simple_time_analyzers:
            if n_vehicle_legs == 0:
                valids = []
            else:
                valids = compute_pareto_front([LabelTimeSimple(label.departure_time, label.arrival_time_target)
                                                 for label in self.all_labels
                                                 if label.n_vehicle_legs <= n_vehicle_legs])
            valids.sort(key=lambda label: -label.departure_time)
            profile = NodeProfileSimple(self._walk_to_target_duration)
            for valid in valids:
                profile.update_pareto_optimal_tuples(valid)
            npat = NodeProfileAnalyzerTime(profile, self.start_time_dep, self.end_time_dep)
            self._n_veh_legs_to_simple_time_analyzers[n_vehicle_legs] = npat
        return self._n_veh_legs_to_simple_time_analyzers[n_vehicle_legs]

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
        min_n = self.min_trip_n_boardings()
        if max_n is None:
            return None
        fig = plt.figure()
        ax = fig.add_subplot(111)
        from matplotlib import cm
        viridis = cm.get_cmap("viridis_r")
        step = min(0.3, 0.9 / (float(max_n - min_n + 1)))
        colors = [viridis(step * (i - min_n)) for i in reversed(range(min_n, max_n + 1))]
        max_temporal_distance = 0
        for color, n_vehicle_legs in zip(colors, range(min_n, max_n + 1)):
            npat = self._get_time_profile_analyzer(n_vehicle_legs)
            maxdist = npat.largest_finite_temporal_distance()
            if maxdist is not None and maxdist > max_temporal_distance:
                max_temporal_distance = maxdist
            linewidth = 0.5 + 3 * (n_vehicle_legs / max(1.0, float(max_n)))
            if n_vehicle_legs == max_n:
                label = "fastest possible using at most " + str(n_vehicle_legs) + " vehicle(s)"
            else:
                label = "time lost using " + str(n_vehicle_legs) + " vehicle(s) instead of " + str(n_vehicle_legs + 1)

            npat.plot_temporal_distance_variation(timezone=timezone,
                                                  color=color,
                                                  alpha=1.0,
                                                  ax=ax,
                                                  lw=linewidth,
                                                  label=label)
        ax.legend(loc="best", framealpha=0.5)
        ax.set_ylim(bottom=0, top=max_temporal_distance / 60.0)
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
            NPA.min_trip_n_boardings,
            NPA.max_trip_n_boardings,
            NPA.mean_trip_n_boardings,
            NPA.median_trip_n_boardings
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
            "min_trip_n_veh_legs",
            "max_trip_n_veh_legs",
            "mean_trip_n_veh_legs",
            "median_trip_n_veh_legs"
        ]
        assert(len(profile_summary_methods) == len(profile_observable_names))
        return profile_summary_methods, profile_observable_names
