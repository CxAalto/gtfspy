import copy

from gtfspy.routing.util import compute_pareto_front
from gtfspy.routing.label import Label


class NodeProfileC:
    """
    In the connection scan algorithm, each stop has a profile entry
    that stores information on the Pareto-Optimal (departure_time_this_node, arrival_time_target_node) tuples.
    """

    def __init__(self, walk_to_target_duration=float('inf')):
        self._labels = []  # list[Label] # always ordered by decreasing departure_time
        self._walk_to_target_duration = walk_to_target_duration

    def get_walk_to_target_duration(self):
        return self._walk_to_target_duration

    def update_pareto_optimal_tuples(self, new_label):
        """
        Parameters
        ----------
        new_label: Label

        Returns
        -------
        updated: bool
        """
        assert (isinstance(new_label, Label))
        if self._labels:
            assert (new_label.departure_time <= self._labels[-1].departure_time)
            best_later_departing_arrival_time = self._labels[-1].arrival_time_target
        else:
            best_later_departing_arrival_time = float('inf')

        walk_to_target_arrival_time = new_label.departure_time + self._walk_to_target_duration

        best_arrival_time = min(walk_to_target_arrival_time,
                                best_later_departing_arrival_time,
                                new_label.arrival_time_target)
        # this should be changed to get constant time insertions / additions
        # (with time-indexing)
        if (new_label.arrival_time_target < walk_to_target_arrival_time and
                new_label.arrival_time_target < best_later_departing_arrival_time):
            self._labels.append(Label(new_label.departure_time, best_arrival_time))
            return True
        else:
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
        for label in self._labels[::-1]:
            if label.departure_time >= dep_time + transfer_margin:
                minimum = min(minimum, label.arrival_time_target)
                break
        return float(minimum)

    def get_pareto_optimal_tuples(self):
        non_walk_valid_labels = []
        for label in self._labels:
            if label.duration() < self._walk_to_target_duration:
                non_walk_valid_labels.append(label)
        return copy.deepcopy(compute_pareto_front(non_walk_valid_labels))
