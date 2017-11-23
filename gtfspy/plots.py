import matplotlib.pyplot as plt

import pandas

"""
A collection of various useful plots.
"""


def plot_trip_counts_per_day(G, ax=None, higlight_dates=None, highlight_date_labels=None, show=False):
    """
    Parameters
    ----------
    G: gtfspy.GTFS
    ax: maptlotlib.Axes, optional
    higlight_dates: list[str|datetime.datetime]
        The values of highlight dates should represent dates, and  or datetime objects.
    highlight_date_labels: list
        The labels for each highlight dates.
    show: bool, optional
        whether or not to immediately show the results

    Returns
    -------
    ax: maptlotlib.Axes object
    """
    daily_trip_counts = G.get_trip_counts_per_day()
    if ax is None:
        _fig, ax = plt.subplots()
    daily_trip_counts["datetime"] = pandas.to_datetime(daily_trip_counts["date_str"])
    daily_trip_counts.plot("datetime", "trip_counts", kind="line", ax=ax, marker="o", color="C0", ls=":",
                           label="Trip counts")
    ax.set_xlabel("Date")
    ax.set_ylabel("Trip counts per day")
    if higlight_dates is not None:
        assert isinstance(higlight_dates, list)
        if highlight_date_labels is not None:
            assert isinstance(highlight_date_labels, list)
            assert len(higlight_dates) == len(highlight_date_labels), "Number of highlight date labels do not match"
        else:
            highlight_date_labels = [None] * len(higlight_dates)
        for i, (highlight_date, label) in enumerate(zip(higlight_dates, highlight_date_labels)):
            color = "C" + str(int(i % 8 + 1))
            highlight_date = pandas.to_datetime(highlight_date)
            plt.axvline(highlight_date, color=color, label=label)
    ax.legend(loc="best")
    ax.grid()
    if show:
        plt.show()
    return ax
