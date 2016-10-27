def compute_pareto_front(label_list):
    """
    Computes the Pareto frontier of a given label_list

    Parameters
    ----------
    label_list: list[ParetoTuple]
        (Or any list of objects, for which a function label.dominates(other) is defined.

    Returns
    -------
    pareto_front: list[ParetoTuple]
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