import numpy

from gtfspy.routing.label import LabelTimeWithBoardingsCount, merge_pareto_frontiers, compute_pareto_front, LabelVehLegCount, \
    LabelTime


class NodeProfileMultiObjective:
    """
    In the multi-objective connection scan algorithm,
    each stop has a profile entry containing all Pareto-optimal entries.
    """

    def __init__(self,
                 dep_times=None,
                 walk_to_target_duration=float('inf'),
                 label_class=LabelTimeWithBoardingsCount,
                 transit_connection_dep_times=None):
        """

        Parameters
        ----------
        dep_times
        walk_to_target_duration
        label_class: label class to be used
        transit_connection_dep_times:
            if not given, all connections are assumed to be real connections
        """
        if dep_times is None:
            dep_times = []
        self._departure_times = list(reversed(sorted(dep_times)))
        self.dep_times_to_index = dict(zip(self._departure_times, range(len(self._departure_times))))
        self._label_bags = [[]] * len(self._departure_times)
        self._walk_to_target_duration = walk_to_target_duration
        self._min_dep_time = float('inf')
        self.label_class = label_class
        if transit_connection_dep_times is not None:
            self._connection_dep_times = transit_connection_dep_times
        else:
            self._connection_dep_times = dep_times
        assert(isinstance(self._connection_dep_times, (list, numpy.ndarray)))
        self._closed = False
        self._finalized = False
        self._final_pareto_optimal_labels = None
        self._real_connection_labels = None

    def _update_min_dep_time(self, dep_time):
        """
        A simple checker, that connections are coming in descending order of departure time.

        Parameters
        ----------
        dep_time

        Returns
        -------
        None
        """
        assert dep_time <= self._min_dep_time, "Labels should be entered in increasing order of departure time."
        self._min_dep_time = dep_time

    def get_walk_to_target_duration(self):
        """
        Get walking distance to target node.

        Returns
        -------
        walk_to_target_duration: float
        """
        return self._walk_to_target_duration

    def update(self, new_labels, departure_time_backup=None):
        """
        Update the profile with the new labels.
        Each new label should have the same departure_time.

        Parameters
        ----------
        new_labels: list[LabelTime]

        Returns
        -------
        added: bool
            whether new_pareto_tuple was added to the set of pareto-optimal tuples
        """
        if self._closed:
            raise RuntimeError("Profile is closed, no updates can be made")

        try:
            departure_time = next(iter(new_labels)).departure_time
        except StopIteration:
            departure_time = departure_time_backup
        for new_label in new_labels:
            assert(new_label.departure_time == departure_time)
        dep_time_index = self.dep_times_to_index[departure_time]

        if dep_time_index > 0:
            mod_prev_labels = [label.get_copy_with_specified_departure_time(departure_time) for label
                                in self._label_bags[dep_time_index - 1]]
        else:
            mod_prev_labels = list()
        mod_prev_labels += self._label_bags[dep_time_index]

        walk_label = self.get_walk_label(departure_time)
        if walk_label:
            new_labels = new_labels + [walk_label]
        new_frontier = merge_pareto_frontiers(new_labels, mod_prev_labels)

        self._label_bags[dep_time_index] = new_frontier
        self._update_min_dep_time(departure_time)
        return True

    def evaluate(self, dep_time, first_leg_can_be_walk=True, connection_arrival_time=None):
        """
        Get the pareto_optimal set of Labels, given a departure time.

        Parameters
        ----------
        dep_time : float, int
            time in unix seconds
        first_leg_can_be_walk : bool, optional
            whether to allow walking to target to be included into the profile
            (I.e. whether this function is called when scanning a pseudo-connection:
            "double" walks are not allowed.)
        connection_arrival_time: float, int, optional
            if dep_time is float('inf') or ... i.e. this is some sort of fallback

        Returns
        -------
        pareto_optimal_labels : set
            Set of Labels
        """
        pareto_optimal_labels = list()
        # walk label towards target
        if first_leg_can_be_walk and self._walk_to_target_duration != float('inf'):
            # add walk_label
            if connection_arrival_time is not None:
                pareto_optimal_labels.append(self.get_walk_label(connection_arrival_time))
            else:
                pareto_optimal_labels.append(self.get_walk_label(dep_time))
            if pareto_optimal_labels[0] is None:
                assert(pareto_optimal_labels[0] is not None)

        # if dep time is larger than the largest dep time -> only walk labels are possible
        if dep_time in self.dep_times_to_index:
            assert(dep_time != float('inf'))
            index = self.dep_times_to_index[dep_time]
            pareto_optimal_labels = merge_pareto_frontiers(self._label_bags[index], pareto_optimal_labels)
        if not first_leg_can_be_walk:
            pareto_optimal_labels = [label for label in pareto_optimal_labels if not label.first_leg_is_walk]
        return pareto_optimal_labels

    def get_walk_label(self, departure_time):
        if departure_time != float('inf') and self._walk_to_target_duration != float('inf'):
            if self._walk_to_target_duration == 0:
                first_leg_is_walk = False
            else:
                first_leg_is_walk = True
            return self.label_class(departure_time=departure_time,
                                    arrival_time_target=departure_time + self._walk_to_target_duration,
                                    n_boardings=0,
                                    first_leg_is_walk=first_leg_is_walk)
        else:
            return None

    def get_labels_for_real_connections(self):
        self._closed = True
        if self._real_connection_labels is None:
            self._compute_real_connection_labels()
        return self._real_connection_labels

    def get_final_optimal_labels(self):
        """
        Get pareto-optimal labels.

        Returns
        -------
        """
        assert self._finalized, "finalize() first!"
        return self._final_pareto_optimal_labels

    def finalize(self, neighbor_label_bags=None, walk_durations=None):
        """
        Parameters
        ----------
        neighbor_label_bags: list
            each list element is a list of labels corresponding to a neighboring node
             (note: only labels with first connection being a departure should be included)
        walk_durations: list

        Returns
        -------
        None
        """
        assert(not self._finalized)
        if self._final_pareto_optimal_labels is None:
            self._compute_real_connection_labels()
        if neighbor_label_bags is not None:
            assert(len(walk_durations) == len(neighbor_label_bags))
            self._update_final_pareto_optimal_label_set(neighbor_label_bags, walk_durations)
        else:
            self._final_pareto_optimal_labels = self._real_connection_labels
        self._finalized = True
        self._closed = True

    def _compute_real_connection_labels(self):
        pareto_optimal_labels = []
        # do not take those bags with first event is a pseudo-connection
        for dep_time in self._connection_dep_times:
            index = self.dep_times_to_index[dep_time]
            pareto_optimal_labels.extend([label for label in self._label_bags[index] if not label.first_leg_is_walk])
        if self.label_class == LabelTimeWithBoardingsCount or self.label_class == LabelTime:
            pareto_optimal_labels = [label for label in pareto_optimal_labels
                                     if label.duration() < self._walk_to_target_duration]

        if self.label_class == LabelVehLegCount and self._walk_to_target_duration < float('inf'):
            pareto_optimal_labels.append(LabelVehLegCount(0))
        self._real_connection_labels = [label.get_copy() for label in compute_pareto_front(pareto_optimal_labels,
                                                                                           finalization=True)]

    def _update_final_pareto_optimal_label_set(self, neighbor_label_bags, walk_durations):
        labels_from_neighbors = []
        for label_bag, walk_duration in zip(neighbor_label_bags, walk_durations):
            for label in label_bag:
                labels_from_neighbors.append(label.get_copy_with_walk_added(walk_duration))
        self._final_pareto_optimal_labels = compute_pareto_front(self._real_connection_labels +
                                                                 labels_from_neighbors,
                                                                 finalization=True)
