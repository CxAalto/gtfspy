from gtfspy.routing.fastest_path_analyzer import FastestPathAnalyzer


def add_transfer_penalties_to_arrival_times(journey_labels, penalty_seconds, ignore_first_boarding=True):
    """
    Add a fixed transfer penalty for each transfer made.

    Parameters
    ----------
    journey_labels: list[JourneyLabelType]
    penalty_seconds: int
    ignore_first_boarding: bool
        Whether or not one should penalize for the first boarding that does not
        require any transfers altogether.

    Returns
    -------
    new_labels: list[JourneyLabelType]
    """
    new_labels = []
    for label in journey_labels:
        assert hasattr(label, "n_boardings")
        assert hasattr(label, "arrival_time_target")
        new_label = label.get_copy()
        if ignore_first_boarding:
            new_label.arrival_time_target += max(0, label.n_boardings - 1) * penalty_seconds
        else:
            new_label.arrival_time_target += label.n_boardings * penalty_seconds
        # new_label.n_boardings = -1  # To mark that boarding counts have been taken away.
        new_labels.append(new_label)
    return new_labels


def get_fastest_path_analyzer_after_transfer_penalties(labels,
                                                       start_time_dep,
                                                       end_time_dep,
                                                       transfer_penalty_seconds,
                                                       ignore_first_boarding=True,
                                                       walk_duration=float('inf'),
                                                       label_props_to_consider=None,
                                                       **kwargs):
    transfer_penalized_labels = add_transfer_penalties_to_arrival_times(labels, transfer_penalty_seconds, ignore_first_boarding)
    fpa = FastestPathAnalyzer(transfer_penalized_labels,
                              start_time_dep,
                              end_time_dep,
                              walk_duration=walk_duration,
                              label_props_to_consider=label_props_to_consider,
                              **kwargs)
    return fpa


