from __future__ import print_function
import matplotlib.pyplot as plt

import numpy as np
from gtfspy.routing.node_profile import NodeProfile


def plot_temporal_distance_variation(profile, start_time=None, end_time=None, show=False):
    """
    Parameters
    ----------
    profile: NodeProfile
    start_time: int, optional
    end_time: int, optional
    show: bool, optional
        defaults to False (whether or not to display the figure on-screen)

    Returns
    -------
    fig: matplotlib.figure
    """
    pareto_tuples = list(profile.get_pareto_tuples())
    pareto_tuples.sort(key=lambda pt: pt.departure_time)  # , reverse=True)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    departure_times = np.array(map(lambda pt: pt.departure_time, pareto_tuples))
    arrival_times = np.array(map(lambda pt: pt.arrival_time_target, pareto_tuples))

    if start_time is None:
        start_time = departure_times[0]
    if end_time is None:
        end_time = departure_times[-1]

    vlines = []
    slopes = []
    previous_dep_time = None
    for i, (departure_time, arrival_time) in enumerate(zip(departure_times, arrival_times)):
        if departure_time < start_time:
            continue
        if departure_time > end_time:
            break
        if previous_dep_time is None:
            previous_dep_time = start_time
            previous_arr_time = None
        else:
            previous_dep_time = departure_times[i - 1]
            previous_arr_time = arrival_times[i - 1]
            previous_duration = previous_arr_time - previous_dep_time
        duration = arrival_time - departure_time
        print(duration)
        gap = departure_time - previous_dep_time

        duration_after_previous_departure = duration + gap
        slope = dict(x=[previous_dep_time, departure_time], y=[duration_after_previous_departure, duration])
        slopes.append(slope)

        if previous_arr_time is not None:
            vlines.append(dict(x=[previous_dep_time, previous_dep_time],
                               y=[previous_duration, duration_after_previous_departure]))

    for line in slopes:
        ax.plot(line['x'], line['y'], "-", color="black")
    for line in vlines:
        ax.plot(line['x'], line['y'], "--", color="black")

    assert(isinstance(ax, plt.Axes))

    fill_between_x = []
    fill_between_y = []
    for line in slopes:
        fill_between_x.extend(line["x"])
        fill_between_y.extend(line["y"])
    ax.fill_between(fill_between_x, y1=fill_between_y, color="red", alpha=0.2)

    ax.set_ylim(bottom=0)
    ax.set_xlim(start_time, end_time)
    ax.set_xlabel("Departure time")
    ax.set_ylabel("Duration to destination")
    if show:
        plt.show()



