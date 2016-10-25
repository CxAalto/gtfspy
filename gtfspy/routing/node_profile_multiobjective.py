import copy
from collections import OrderedDict

from gtfspy.routing.label import Label, LabelWithTransfers, merge_pareto_frontiers, compute_pareto_front


class NodeProfileMultiObjective:
    """
    In the multiobjective connection scan algorithm,
    each stop has a profile entry containing all Pareto-optimal entries.
    """

    def __init__(self, walk_to_target_duration=float('inf'), label_class=Label):
        self._dep_time_to_index = OrderedDict()
        self._label_bags = []
        self._walk_to_target_duration = walk_to_target_duration
        self._min_dep_time = float('inf')
        self._label_class=label_class

    def _update_min_dep_time(self, dep_time):
        assert(dep_time <= self._min_dep_time, "Labels should be entered in increasing order of departure time.")
        self._min_dep_time = dep_time

    def get_walk_to_target_duration(self):
        return self._walk_to_target_duration

    def update(self, new_labels):
        """
        This function could most likely be optimized.

        Parameters
        ----------
        new_labels: Label, set[Label]

        Returns
        -------
        added: bool
            whether new_pareto_tuple was added to the set of pareto-optimal tuples
        """
        if not isinstance(new_labels, set):
            new_labels = {new_labels}

        if not new_labels:
            return False

        departure_time = next(iter(new_labels)).departure_time
        self._update_min_dep_time(departure_time)

        if self._label_bags:
            previous_labels = self._label_bags[-1]
        else:
            previous_labels = set()

        mod_prev_labels = {label.get_copy_with_specified_departure_time(departure_time) for label
                           in previous_labels}
        if self._walk_to_target_duration != float('inf'):
            mod_prev_labels.add(self._label_class.direct_walk_label(departure_time, self._walk_to_target_duration))

        new_frontier = merge_pareto_frontiers(new_labels, mod_prev_labels)
        if departure_time in self._dep_time_to_index:
            self._label_bags[-1] = new_frontier
        else:
            self._dep_time_to_index[departure_time] = len(self._label_bags)
            self._label_bags.append(new_frontier)
        return True

    def evaluate(self, dep_time, transfer_margin):
        """
        Get the pareto_optimal set of Labels, given a departure time.

        Parameters
        ----------
        dep_time : float, int
            time in unix seconds
        transfer_margin: float, int
            transfer margin in seconds

        Returns
        -------
        pareto_optimal_labels : set
            Set of Labels
        """
        pareto_optimal_labels = set()
        if self._walk_to_target_duration != float('inf'):
            walk_pareto_tuple = LabelWithTransfers(departure_time=dep_time,
                                                   arrival_time_target=dep_time + self._walk_to_target_duration,
                                                   n_transfers=0)
            pareto_optimal_labels.add(walk_pareto_tuple)

        dep_time_plus_transfer_margin = dep_time + transfer_margin
        # self._pareto_tuples is ordered in increasing departure time
        for dep_time, index in reversed(self._dep_time_to_index.items()):
            if dep_time >= dep_time_plus_transfer_margin:
                return self._label_bags[index]
        return set()

    def get_pareto_optimal_tuples(self):
        pareto_optimal_tuples = list()
        for bag in self._label_bags:
            pareto_optimal_tuples.extend(bag)
        return copy.deepcopy(compute_pareto_front(pareto_optimal_tuples))

