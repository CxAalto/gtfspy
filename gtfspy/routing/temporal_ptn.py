

class TemporalPTN:

    def __init__(self, events, node_info, simulation_params):
        self.events = events
        self.node_info = node_info
        self.simulation_params = simulation_params

    def set_simulation_params(self):
        pass

    def random_shift_route_timetables(self, seconds):
        pass

    def shuffle_events(self):
        pass

    def compute_travel_times(self, origin_destinations, start_time):
        """
        Parameters
        ----------
        origin_destinations: list[tuple]

        start_time: int
            start time given in unixtime

        Returns
        -------
        travel_times: list[float]
            list of travel times in the same order as origin_destinations
        """
        pass

    def disable_links_with_routes(self, route_ids):
        pass

    def disable_link_between_stops(self, from_stop, to_stop):
        pass

    def disable_travel_mode(self):
        pass
