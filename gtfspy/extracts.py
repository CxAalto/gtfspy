import sys
import networkx

# the following is required when using this module as a script
# (i.e. using the if __name__ == "__main__": part at the end of this file)
if __name__ == '__main__' and __package__ is None:
    # import gtfspy
    __package__ = 'gtfspy'


def stop_to_stop_network(gtfs):
    """
    First priority:
        raw data, individual stops, directed
    Link attributes:
        From node
        To node
        Number of vehicles passed
        Approximate capacity passed
        Average travel time between stops
        Straight-line distance
        List of lines, separated with a
        Node attributes:
        ID
        Coordinates
        Name of the stop
        Data format to be used:
        Edge file (i, j, vehicle count, capacity, travel time, distance)
    """
    pass


def multi_layer_network(gtfs):
    """
    Stop-to-stop networks + layers reflecting modality
        Ask Mikko for more details?
        Separate networks for each mode.
        Modes:
        Walking + GTFS

    Parameters
    ----------
    gtfs

    Returns
    -------
    ?

    """
    pass


def multilayer_temporal_network(gtfs):
    """
    Parameters
    ----------
    gtfs

    Returns
    -------
    multilayer_temporal_network? :)
    """
    pass


def line_to_line_network(gtfs):
    """
    Parameters
    ----------
    gtfs

    Returns
    -------

    """
    pass


def main(cmd, args):
    if cmd == "directed_network":
        net = ext


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])

