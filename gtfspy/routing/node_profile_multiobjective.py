from collections import OrderedDict

from gtfspy.routing.label import LabelTimeAndVehLegCount, merge_pareto_frontiers, compute_pareto_front, LabelVehLegCount


class NodeProfileMultiObjective:
    """
    In the multi-objective connection scan algorithm,
    each stop has a profile entry containing all Pareto-optimal entries.
    """

    def __init__(self, walk_to_target_duration=float('inf'), label_class=LabelTimeAndVehLegCount):
        self._dep_time_to_index = OrderedDict()
        self._departure_times = []
        self._label_bags = []
        self._walk_to_target_duration = walk_to_target_duration
        self._min_dep_time = float('inf')
        self.label_class = label_class

    def _update_min_dep_time(self, dep_time):
        assert dep_time <= self._min_dep_time, "Labels should be entered in increasing order of departure time."
        self._min_dep_time = dep_time

    def get_walk_to_target_duration(self):
        return self._walk_to_target_duration

    def update(self, new_labels):
        """
        Update the profile with the new labels.
        Each new label should have the same departure_time.

        Parameters
        ----------
        new_labels: LabelTime, set[LabelTime]

        Returns
        -------
        added: bool
            whether new_pareto_tuple was added to the set of pareto-optimal tuples
        """
        if not isinstance(new_labels, list):
            new_labels = [new_labels]

        if not new_labels:
            return False

        departure_time = next(iter(new_labels)).departure_time
        for new_label in new_labels:
            assert(new_label.departure_time == departure_time)

        if self._label_bags:
            previous_labels = self._label_bags[-1]
        else:
            previous_labels = list()

        mod_prev_labels = [label.get_copy_with_specified_departure_time(departure_time) for label
                           in previous_labels]

        if self._walk_to_target_duration != float('inf'):
            mod_prev_labels.append(self.label_class.direct_walk_label(departure_time, self._walk_to_target_duration))

        new_frontier = merge_pareto_frontiers(new_labels, mod_prev_labels)

        if self._min_dep_time == departure_time:
            self._label_bags[-1] = new_frontier
        else:
            self._dep_time_to_index[departure_time] = len(self._label_bags)
            self._label_bags.append(new_frontier)
            self._departure_times.append(departure_time)
        self._update_min_dep_time(departure_time)
        return True

    def evaluate(self, dep_time, transfer_margin=0, allow_walk_to_target=True):
        """
        Get the pareto_optimal set of Labels, given a departure time.

        Parameters
        ----------
        dep_time : float, int
            time in unix seconds
        transfer_margin: float, int
            transfer margin in seconds
        allow_walk_to_target : bool, optional
            whether to allow walking to target to be included into the profile
            (I.e. whether this function is called when scanning a pseudo-connection:
            "double" walks are not allowed.)

        Returns
        -------
        pareto_optimal_labels : set
            Set of Labels
        """
        pareto_optimal_labels = list()
        if self._walk_to_target_duration != float('inf') and allow_walk_to_target:
            walk_pareto_tuple = self.label_class(departure_time=dep_time,
                                                 arrival_time_target=dep_time + self._walk_to_target_duration)
            pareto_optimal_labels.append(walk_pareto_tuple)

        dep_time_plus_transfer_margin = dep_time + transfer_margin

        for dep_time, index in reversed(self._dep_time_to_index.items()):
            # TODO! Optimize this, if really needed
            if dep_time >= dep_time_plus_transfer_margin:
                pareto_optimal_labels = merge_pareto_frontiers(self._label_bags[index], pareto_optimal_labels)
                break
        return pareto_optimal_labels

    def get_pareto_optimal_labels(self):
        # there may be some room for optimization here
        pareto_optimal_labels = []
        for bag in self._label_bags:
            pareto_optimal_labels.extend(bag)
        if self.label_class == LabelVehLegCount and self._walk_to_target_duration < float('inf'):
            pareto_optimal_labels.append(LabelVehLegCount(0))
        return [label.get_copy() for label in compute_pareto_front(pareto_optimal_labels)]

