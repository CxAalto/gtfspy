import copy

from gtfspy.routing.pareto_tuple import ParetoTuple
from gtfspy.routing.util import compute_pareto_front


class NodeProfileC:
    """
    In the connection scan algorithm, each stop has a profile entry
    that stores information on the Pareto-Optimal (departure_time_this_node, arrival_time_target_node) tuples.
    """

    def __init__(self, walk_to_target_duration=float('inf')):
        self._pareto_tuples = []  # list[ParetoTuple] # always ordered by decreasing departure_time
        self._walk_to_target_duration = walk_to_target_duration

    def get_walk_to_target_duration(self):
        return self._walk_to_target_duration

    def update_pareto_optimal_tuples(self, new_pareto_tuple):
        """
        Parameters
        ----------
        new_pareto_tuple: ParetoTuple
        """
        assert(isinstance(new_pareto_tuple, ParetoTuple))
        if self._pareto_tuples:
            assert(new_pareto_tuple.departure_time <= self._pareto_tuples[-1].departure_time)
        walk_to_target_arrival_time = new_pareto_tuple.departure_time + self._walk_to_target_duration
        if self._pareto_tuples:
            best_later_departing_arrival_time = self._pareto_tuples[-1].arrival_time_target
        else:
            best_later_departing_arrival_time = float('inf')
        best_arrival_time = min(walk_to_target_arrival_time,
                                best_later_departing_arrival_time,
                                new_pareto_tuple.arrival_time_target)
        print
        self._pareto_tuples.append(ParetoTuple(new_pareto_tuple.departure_time, best_arrival_time))

    def evaluate_earliest_arrival_time_at_target(self, dep_time, transfer_margin):
        """
        Get the earliest arrival time at the target, given a departure time.
        "Evaluate a profile"

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
        for pt in self._pareto_tuples[::-1]:
            print(minimum, pt)
            if pt.departure_time >= dep_time + transfer_margin:
                minimum = min(minimum, pt.arrival_time_target)
                break
        return float(minimum)

    def get_pareto_tuples_for_analysis(self):
        non_walk_valid_pareto_tuples = []
        for pt in self._pareto_tuples:
            if pt.duration() < self._walk_to_target_duration:
                non_walk_valid_pareto_tuples.append(pt)
        return copy.deepcopy(compute_pareto_front(non_walk_valid_pareto_tuples))


