from __future__ import print_function

import numpy
import matplotlib.pyplot as plt

from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTimeAndVehLegCount, compute_pareto_front, LabelTime
from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime
from gtfspy.routing.node_profile_simple import NodeProfileSimple


def _if_no_labels_return_none(func):
    def wrapper(self):
        print("args", self)
        if self._labels_within_time_frame:
            return func(self)
        else:
            return None
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
        self.all_labels.extend(self.node_profile.evaluate(end_time_dep, 0))
        self._labels_within_time_frame = self.all_labels[::-1]

        self._walk_to_target_duration = self.node_profile.get_walk_to_target_duration()

    @_if_no_labels_return_none
    def max_trip_n_veh_legs(self):
        return numpy.max([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_if_no_labels_return_none
    def min_trip_n_veh_legs(self):
        return numpy.min([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_if_no_labels_return_none
    def mean_trip_n_veh_legs(self):
        return numpy.mean([label.n_vehicle_legs for label in self._labels_within_time_frame])

    @_if_no_labels_return_none
    def median_trip_n_veh_legs(self):
        return numpy.median([label.n_vehicle_legs for label in self._labels_within_time_frame])

    def plot_temporal_distance_variation(self, timezone=None):
        max_n = self.max_trip_n_veh_legs()
        min_n = self.min_trip_n_veh_legs()
        if max_n is None:
            return None
        fig = plt.figure()
        ax = fig.add_subplot(111)
        for n_vehicle_legs in range(min_n, max_n + 1):
            valids = compute_pareto_front([LabelTime(label.departure_time, label.arrival_time_target)
                                           for label in self.all_labels
                                           if label.n_vehicle_legs <= n_vehicle_legs])
            valids.sort(key=lambda label: -label.departure_time)
            profile = NodeProfileSimple(self._walk_to_target_duration)
            for valid in valids:
                profile.update_pareto_optimal_tuples(valid)
            npat = NodeProfileAnalyzerTime(profile, self.start_time_dep, self.end_time_dep)
            npat.plot_temporal_distance_variation(timezone=timezone, color="red", ax=ax)
        return fig



