from collections import namedtuple

_pareto_tuple = namedtuple('ParetoTuple',
                           ['departure_time', 'arrival_time_target'])


class ParetoTuple(_pareto_tuple):
    def dominates(self, other):
        """
        Compute whether this ParetoTuple dominates the other ParetoTuple

        Parameters
        ----------
        other: ParetoTuple

        Returns
        -------
        dominates: bool
            True if this ParetoTuple dominates the other, otherwise False
        """
        dominates = (
            (self.departure_time >= other.departure_time and self.arrival_time_target < other.arrival_time_target) or
            (self.departure_time > other.departure_time and self.arrival_time_target <= other.arrival_time_target)
        )
        return dominates

    def duration(self):
        """
        Get trip duration.

        Returns
        -------
        duration: float

        """
        return self.arrival_time_target - self.departure_time


_pareto_tuple_with_tranfers = namedtuple('ParetoTuple',
                                         ['departure_time', 'arrival_time_target', "n_transfers"])


class ParetoTupleWithTransfers(_pareto_tuple_with_tranfers):

    def dominates(self, other):
        """
        Compute whether this ParetoTuple dominates the other ParetoTuple

        Parameters
        ----------
        other: ParetoTupleWithTransfers

        Returns
        -------
        dominates: bool
            True if this ParetoTuple dominates the other, otherwise False
        """
        all_better_or_equal = (
            self.departure_time >= other.departure_time and
            self.arrival_time_target <= other.arrival_time_target and
            self.n_transfers <= other.n_transfers
        )
        all_equal = (
            self.departure_time == other.departure_time and
            self.arrival_time_target == other.arrival_time_target and
            self.n_transfers == other.n_transfers
        )
        dominates = all_better_or_equal and (not all_equal)
        return dominates

    def duration(self):
        """
        Get trip duration.

        Returns
        -------
        duration: float

        """
        return self.arrival_time_target - self.departure_time
