import sys

import networkx

# the following is required when using this module as a script
# (i.e. using the if __name__ == "__main__": part at the end of this file)
from gtfs import GTFS


def stop_to_stop_network(gtfs):
    """
    Extract a stop-to-stop network from the

    Returns
    -------
    networkx.DiGraph

    """
    graph = networkx.DiGraph()
    stops = gtfs.get_stop_info()
    for stop in stops.itertuples():
        attr_dict = {
            "lat": stop.lat,
            "lon": stop.lon,
            "name": stop.name
        }
        graph.add_node(stop.stop_I, attr_dict=attr_dict)
    # get all
    segments =
    for segment in segments:
        # get all stop times between the segments, and compute the mean travel time
        # get the distance
        # for each segement, get the total travel time
        # for each seg
    return graph

def get_walk_network(self):
    """
    Get the walk network.

    Returns
    -------
    networkx.DiGraph
    """




def extract_multi_layer_network(self):
    """
    Stop-to-stop networks + layers reflecting modality
        Ask Mikko for more details?
        Separate networks for each mode.
        Modes:
        Walking + GTFS
    """
    pass

def extract_multilayer_temporal_network(self):
    pass

def line_to_line_network(self):
    pass
