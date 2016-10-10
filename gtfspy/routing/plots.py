from __future__ import print_function

import numpy as np

from gtfspy.routing.analyses.node_profile_analyzer import NodeProfileAnalyzer
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
    departure_times = np.array(map(lambda pt: pt.departure_time, pareto_tuples))
    if start_time is None:
        start_time = departure_times[0]
    if end_time is None:
        end_time = departure_times[-1]
    analyzer = NodeProfileAnalyzer(profile, start_time, end_time)
    analyzer.plot_temporal_variation(show=show, timezone=timezone)



