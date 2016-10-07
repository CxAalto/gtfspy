from __future__ import print_function

import datetime

import matplotlib.pyplot as plt
from matplotlib import dates as md

import numpy as np
import pytz

from gtfspy.routing.node_profile import NodeProfile


def plot_temporal_distance_variation(profile, start_time=None, end_time=None, show=False, timezone=None):
    """
    Parameters
    ----------
    profile: NodeProfile
    start_time: int, optional
    end_time: int, optional
    show: bool, optional
        defaults to False (whether or not to display the figure on-screen)
    timezone: pytz.timezone
        datetime.tzinfo


    Returns
    -------
    fig: matplotlib.pyplot.Figure
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
            previous_duration_minutes = (previous_arr_time - previous_dep_time) / 60.0
        duration_minutes = (arrival_time - departure_time) / 60.0
        gap_minutes = (departure_time - previous_dep_time) / 60.0

        duration_after_previous_departure_minutes = duration_minutes + gap_minutes
        slope = dict(x=[previous_dep_time, departure_time], y=[duration_after_previous_departure_minutes, duration_minutes])
        slopes.append(slope)

        if previous_arr_time is not None:
            vlines.append(dict(x=[previous_dep_time, previous_dep_time],
                               y=[previous_duration_minutes, duration_after_previous_departure_minutes]))

    if timezone is None:
        print("Warning: No timezone specified, defaulting to UTC")
        timezone = pytz.timezone("Etc/UTC")

    def _ut_to_unloc_datetime(ut):
        dt = datetime.datetime.fromtimestamp(ut, timezone)
        return dt.replace(tzinfo=None)

    format_string = "%Y-%m-%d %H:%M:%S"
    x_axis_formatter = md.DateFormatter(format_string)
    ax.xaxis.set_major_formatter(x_axis_formatter)

    for line in slopes:
        xs = [_ut_to_unloc_datetime(x) for x in line['x']]
        ax.plot(xs, line['y'], "-", color="black")
    for line in vlines:
        xs = [_ut_to_unloc_datetime(x) for x in line['x']]
        ax.plot(xs, line['y'], "--", color="black")

    assert(isinstance(ax, plt.Axes))

    fill_between_x = []
    fill_between_y = []
    for line in slopes:
        xs = [_ut_to_unloc_datetime(x) for x in line['x']]
        fill_between_x.extend(xs)
        fill_between_y.extend(line["y"])

    ax.fill_between(fill_between_x, y1=fill_between_y, color="red", alpha=0.2)

    ax.set_ylim(bottom=0)
    ax.set_xlim(
        _ut_to_unloc_datetime(start_time),
        _ut_to_unloc_datetime(end_time)
    )
    ax.set_xlabel("Departure time")
    ax.set_ylabel("Duration to destination (min)")
    fig.tight_layout()
    plt.xticks(rotation=45)
    plt.subplots_adjust(bottom=0.3)
    if show:
        plt.show()
    return fig


