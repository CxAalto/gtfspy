import copy

from gtfspy.routing.label import Label, LabelWithTransfers


class NodeProfileMultiObjective:
    """
    In the multiobjective connection scan algorithm,
    each stop has a profile entry containing all Pareto-optimal entries.
    """

    def __init__(self, walk_to_target_duration=float('inf')):
        self._pareto_tuples = []  # list[ParetoTuple] # always ordered by decreasing departure_time
        self._walk_to_target_duration = walk_to_target_duration

    def get_walk_to_target_duration(self):
        return self._walk_to_target_duration

    def update_pareto_optimal_tuples(self, new_pareto_tuple):
        """
        # This function should be optimized

        Parameters
        ----------
        new_pareto_tuple: Label or LabelWithTransfers

        Returns
        -------
        added: bool
            whether new_pareto_tuple was added to the set of pareto-optimal tuples
        """
        # Implicit assumption: walking to target dominates all other trips,
        # if the walk_duration just is longer.
        if new_pareto_tuple.duration() >= self._walk_to_target_duration:
            return False

        if self._new_paretotuple_is_dominated_by_old_tuples(new_pareto_tuple):
            return False
        else:
            self._remove_old_tuples_dominated_by_new_and_insert_new_paretotuple(new_pareto_tuple)
            return True

    def _remove_old_tuples_dominated_by_new_and_insert_new_paretotuple(self, new_pareto_tuple):
        indices_to_remove = []
        n = len(self._pareto_tuples)
        insert_location = 0  # default for the case where len(self._pareto_tuples) == 0
        for i in range(n - 1, -1, -1):
            old_tuple = self._pareto_tuples[i]
            if old_tuple.departure_time > new_pareto_tuple.departure_time:
                insert_location = i + 1
                break
            else:
                if new_pareto_tuple.dominates(old_tuple):
                    indices_to_remove.append(i)
        for ix in indices_to_remove:
            del self._pareto_tuples[ix]
        self._pareto_tuples.insert(insert_location, new_pareto_tuple)

    def _new_paretotuple_is_dominated_by_old_tuples(self, new_pareto_tuple):
        # self._pareto_tuples is guaranteed to be ordered in decreasing dep_time
        # new_pareto_tuple can only be dominated by those elements, which have
        # larger or equal dep_time than new_pareto_tuple.
        for old_tuple in self._pareto_tuples[::-1]:
            if old_tuple.departure_time >= new_pareto_tuple.arrival_time_target:
                # all following arrival times are necessarily larger
                # than new_pareto_tuple's arrival time
                break
            else:
                if old_tuple.dominates(new_pareto_tuple):
                    return True
        return False

    def get_pareto_optimal_tuples(self, dep_time, transfer_margin):
        """
        Get the pareto_optimal arrival times at target, given a departure time.

        Parameters
        ----------
        dep_time : float, int
            time in unix seconds
        transfer_margin: float, int
            transfer margin in seconds

        Returns
        -------
        pareto_optimal_labels : set[Label]
            Set of ParetoTuples
        """
        pareto_optimal_labels = set()
        if self._walk_to_target_duration != float('inf'):
            walk_pareto_tuple = LabelWithTransfers(departure_time=dep_time,
                                                   arrival_time_target=dep_time + self._walk_to_target_duration,
                                                   n_transfers=0)
            pareto_optimal_labels.add(walk_pareto_tuple)
        dep_time_plus_transfer_margin = dep_time + transfer_margin
        for pt in self._pareto_tuples:
            if pt.departure_time >= dep_time_plus_transfer_margin and pt.arrival_time_target < minimum:

                minimum = pt.arrival_time_target
        return labels

    def get_pareto_tuples(self):
        return copy.deepcopy(self._pareto_tuples)

