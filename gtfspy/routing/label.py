from collections import namedtuple

_label = namedtuple('Label',
                    ['departure_time', 'arrival_time_target'])


class Label(_label):
    """
    Label describes the entries in a Profile.
    """

    def dominates(self, other):
        """
        Compute whether this ParetoTuple dominates the other ParetoTuple

        Parameters
        ----------
        other: Label

        Returns
        -------
        dominates: bool
            True if this ParetoTuple dominates the other, otherwise False
        """
        dominates = (
            (self.departure_time >= other.departure_time and self.arrival_time_target <= other.arrival_time_target)
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


_label_with_tranfers = namedtuple('ParetoTuple',
                                  ['departure_time', 'arrival_time_target', "n_transfers"])


class LabelWithTransfers(_label_with_tranfers):

    def dominates(self, other):
        """
        Compute whether this ParetoTuple dominates the other ParetoTuple

        Parameters
        ----------
        other: LabelWithTransfers

        Returns
        -------
        dominates: bool
            True if this ParetoTuple dominates the other, otherwise False
        """
        dominates = (
            self.departure_time >= other.departure_time and
            self.arrival_time_target <= other.arrival_time_target and
            self.n_transfers <= other.n_transfers
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


def compute_pareto_front(label_list):
    """
    Computes the Pareto frontier of a given label_list

    Parameters
    ----------
    label_list: list[Label]
        (Or any list of objects, for which a function label.dominates(other) is defined.

    Returns
    -------
    pareto_front: list[Label]
        List of labels that belong to the Pareto front.

    Notes
    -----
    Code adapted from:
    http://stackoverflow.com/questions/32791911/fast-calculation-of-pareto-front-in-python
    """
    dominated = []
    pareto_front = []
    remaining = label_list
    while remaining:  # (is not empty)
        candidate = remaining[0]
        new_remaining = []
        is_dominated = False
        for other in remaining[1:]:
            if candidate.dominates(other):
                dominated.append(other)
            else:
                new_remaining.append(other)
                if other.dominates(candidate):
                    is_dominated = True
        if is_dominated:
            dominated.append(candidate)
        else:
            pareto_front.append(candidate)
        remaining = new_remaining
        # after each round:
        #   remaining contains nodes that are not dominated by any in the pareto_front
        #   dominated contains only nodes that are
        #
    return pareto_front


def merge_pareto_frontiers(labels, labels_other):
    """
    Merge two pareto frontiers by removing dominated entries.

    Parameters
    ----------
    labels: List[Label]
    labels_other: List[Label]

    Returns
    -------
    pareto_front_merged: List[Label]
    """

    labels_survived = []
    labels_other_survived = []

    for label_other in labels_other:
        is_dominated = False
        for label in labels:
            if label.dominates(label_other):
                is_dominated = True
                break
        if not is_dominated:
            labels_other_survived.append(label_other)

    for label in labels:
        is_dominated = False
        for label_other_survived in labels_other_survived:
            if label_other_survived.dominates(label):
                is_dominated = True
                break
        if not is_dominated:
            labels_survived.append(label)

    return labels_survived + labels_other_survived
