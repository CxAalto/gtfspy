from gtfspy.routing.connection import Connection
from gtfspy.networks import temporal_network, walk_transfer_stop_to_stop_network
from gtfspy.gtfs import GTFS
import pandas
from warnings import warn


def get_transit_connections(gtfs, start_time_ut, end_time_ut):
    """
    Parameters
    ----------
    gtfs: gtfspy.GTFS
    end_time_ut: int
    start_time_ut: int

    Returns
    -------
    list[Connection]
    """
    if start_time_ut + 20 * 3600 < end_time_ut:
        warn("Note that it is possible that same trip_I's can take place during multiple days, "
             "which could (potentially) affect the outcomes of the CSA routing!")
    assert (isinstance(gtfs, GTFS))
    events_df = temporal_network(gtfs, start_time_ut=start_time_ut, end_time_ut=end_time_ut)
    assert (isinstance(events_df, pandas.DataFrame))
    return list(map(lambda e: Connection(e.from_stop_I, e.to_stop_I, e.dep_time_ut, e.arr_time_ut, e.trip_I),
                    events_df.itertuples()
                    )
                )


def get_walk_network(gtfs, max_link_distance_m=1000):
    """
    Parameters
    ----------
    gtfs: gtfspy.GTFS

    Returns
    -------
    walk_network: networkx.Graph:
    """
    assert (isinstance(gtfs, GTFS))
    return walk_transfer_stop_to_stop_network(gtfs, max_link_distance=max_link_distance_m)
