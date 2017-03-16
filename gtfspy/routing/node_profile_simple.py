from gtfspy.routing.label import LabelTimeSimple


class NodeProfileSimple:
    """
    In the connection scan algorithm, each stop has a profile entry
    that stores information on the Pareto-Optimal (departure_time_this_node, arrival_time_target_node) tuples.
    """

    def __init__(self, walk_to_target_duration=float('inf'), label_class=LabelTimeSimple):
        self._labels = []  # list[LabelTimeSimple] # always ordered by decreasing departure_time
        self._walk_to_target_duration = walk_to_target_duration
        self._label_class = label_class

    def get_walk_to_target_duration(self):
        return self._walk_to_target_duration

    def update_pareto_optimal_tuples(self, new_pareto_tuple):
        """
        # this function should be optimized

        Parameters
        ----------
        new_pareto_tuple: LabelTimeSimple

        Returns
        -------
        added: bool
            whether new_pareto_tuple was added to the set of pareto-optimal tuples
        """
        if new_pareto_tuple.duration() > self._walk_to_target_duration:
            direct_walk_label = self._label_class.direct_walk_label(new_pareto_tuple.departure_time,
                                                                    self._walk_to_target_duration)
            if not direct_walk_label.dominates(new_pareto_tuple):
                raise
        direct_walk_label = self._label_class.direct_walk_label(new_pareto_tuple.departure_time, self._walk_to_target_duration)
        if direct_walk_label.dominates(new_pareto_tuple):
            return False

        if self._new_paretotuple_is_dominated_by_old_tuples(new_pareto_tuple):
            return False
        else:
            self._remove_old_tuples_dominated_by_new_and_insert_new_paretotuple(new_pareto_tuple)
            return True

    def _remove_old_tuples_dominated_by_new_and_insert_new_paretotuple(self, new_pareto_tuple):
        indices_to_remove = []
        n = len(self._labels)
        insert_location = 0  # default for the case where len(self._pareto_tuples) == 0
        for i in range(n - 1, -1, -1):
            old_tuple = self._labels[i]
            if old_tuple.departure_time > new_pareto_tuple.departure_time:
                insert_location = i + 1
                break
            else:
                if new_pareto_tuple.dominates(old_tuple):
                    indices_to_remove.append(i)
        for ix in indices_to_remove:
            del self._labels[ix]
        self._labels.insert(insert_location, new_pareto_tuple)

    def _new_paretotuple_is_dominated_by_old_tuples(self, new_pareto_tuple):
        # self._pareto_tuples is guaranteed to be ordered in decreasing dep_time
        # new_pareto_tuple can only be dominated by those elements, which have
        # larger or equal dep_time than new_pareto_tuple.
        for old_tuple in self._labels[::-1]:
            if old_tuple.departure_time >= new_pareto_tuple.arrival_time_target:
                # all following arrival times are necessarily larger
                # than new_pareto_tuple's arrival time
                break
            else:
                if old_tuple.dominates(new_pareto_tuple):
                    return True
        return False

    def evaluate_earliest_arrival_time_at_target(self, dep_time, transfer_margin):
        """
        Get the earliest arrival time at the target, given a departure time.

        Parameters
        ----------
        dep_time : float, int
            time in unix seconds
        transfer_margin: float, int
            transfer margin in seconds

        Returns
        -------
        arrival_time : float
            Arrival time in the given time unit (seconds after unix epoch).
        """
        minimum = dep_time + self._walk_to_target_duration
        dep_time_plus_transfer_margin = dep_time + transfer_margin
        for label in self._labels:
            if label.departure_time >= dep_time_plus_transfer_margin and label.arrival_time_target < minimum:
                minimum = label.arrival_time_target
        return float(minimum)

    def get_final_optimal_labels(self):
        return [label.get_copy() for label in self._labels]

