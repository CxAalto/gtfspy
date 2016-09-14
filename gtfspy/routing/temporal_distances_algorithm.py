import networkx as nx
from collections import defaultdict

import time

"""An algorithm for computing temporal distances in public transport networks"""


class TemporalDistancesAlgorithm:
    """An algorithm for computing temporal distances in public transport networks"""

    def __init__(self, transit_events, start_time, end_time,
                 walk_network, transfer_margin, walk_speed):
        """
        Parameters
        ----------
        transit_events: pandas.DataFrame
            Required columns:
                from (int)
                to (int)
                start_time (int, unixtime seconds)
                end_time (int, unixtime seconds)
                trip_id (int, or str)
        start_time : int
            start time in unixtime seconds
        end_time: int
            end time in unixtime seconds
        transfer_margin: int
            required extra margin required for transfers in seconds
        walk_speed: float
            walking speed between stops
        walk_network: networkx.Graph
            each edge should have the walking distance as an attribute
        """
        self._transit_events = transit_events
        self._start_time = start_time
        self._end_time = end_time
        self._walk_network = walk_network
        self._transfer_margin = transfer_margin
        self._walk_speed = walk_speed
        self._has_run = False
        self._temporal_distances = defaultdict(list)
        self._run_time = None
        self.__last_time_of_contact_ij = defaultdict(lambda: float("-inf"))
        self.__latest_time_of_contact_ij = defaultdict(lambda: float("-inf"))
        self.__path_start_time_ij = defaultdict(lambda: float("-inf"))
        self.__path_duration = defaultdict(lambda: 0)
        self.__average_temporal_distance(lambda: 0)

    def run(self):
        """Run the algorithm"""
        if self._has_run:
            raise RuntimeError("Algorithm has already run, please initialize a new algorithm")
        start_time = time.time()
        self._run_algo(self)
        end_time = time.time()
        self._run_time = end_time - start_time
        self._has_run = True

    def get_temporal_distances(self):
        """
        Returns
        -------
        temporal_distances: defaultdict[tuple(int, int)]

        """
        assert self._has_run
        return self._temporal_distances

    def get_run_time(self):
        """

        Returns
        -------
        run_time: float
            running time of the algorithm in seconds
        """
        assert self._has_run
        return self._run_time

    def _run_algo(self):
        for event in self._transit_events.itertuples():
            if (self.__la)
