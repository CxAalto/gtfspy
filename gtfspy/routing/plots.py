from __future__ import print_function

import numpy as np
from matplotlib import pyplot as plt

from gtfspy.routing.node_profile_simple import NodeProfileSimple
from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime


def plot_temporal_distance_variation(profile, start_time=None, end_time=None, show=False, timezone=None):
    """
    Plot the temporal distance variation profile.
    (This is really just a wrapper of NodeProfileAnalyzerTime.plot_temporal_distance_variation)

    Parameters
    ----------
    profile: NodeProfileSimple
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
    pareto_tuples = list(profile.get_pareto_optimal_tuples())
    pareto_tuples.sort(key=lambda pt: pt.departure_time)  # , reverse=True)
    departure_times = np.array(map(lambda pt: pt.departure_time, pareto_tuples))
    if start_time is None:
        start_time = departure_times[0]
    if end_time is None:
        end_time = departure_times[-1]
    analyzer = NodeProfileAnalyzerTime(profile, start_time, end_time)
    fig = analyzer.plot_temporal_distance_variation(timezone=timezone)
    if show:
        plt.show()
    return fig
