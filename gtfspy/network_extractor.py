import sys

# the following is required when using this module as a script
# (i.e. using the if __name__ == "__main__": part at the end of this file)
if __name__ == '__main__' and __package__ is None:
    # import gtfspy
    __package__ = 'gtfspy'

class NetworkExtractor(object):

    def __init__(self, gtfs):
        """
        Parameters
        ----------
        gtfs: GTFS
            the GTFS object used for fetching timetable data

        See the specifications from

        """
        self.gtfs = gtfs

    def stop_to_stop_network(self):
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



def main():
    cmd = sys.argv[1]
    args = sys.argv[2:]
    if cmd == "directed_network":
        extractor = NetworkExtractor(args[0])
        warningsContainer = extractor.

        warningsContainer.print_summary()

if __name__ == "__main__":
    main()

