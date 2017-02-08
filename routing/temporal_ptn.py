

class TemporalPTN:

    def __init__(self, events, node_info, simulation_params):
        # each event should contain:
        # mode, route, trip, from, to, dep_time, arr_time
        self.events = events
        self.node_info = node_info
        self.simulation_params = simulation_params

    def set_simulation_parameters(self):
        pass

    def random_shift_route_timetables(self, seconds):
        # -> schedule data_structure should be organized by routes
        pass

    def shuffle_route_timetables(self):
        # -> schedule data_structure should be organized by routes
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
        # -> schedule data_structure should be organized by events (?)
        pass

    def disable_trip(self, route_ids):
        # -> schedule data_structure should be organized by trip
        pass

    def disable_link_between_stops(self, from_stop, to_stop):
        # -> schedule data_structure should be organized by events
        pass

    def disable_travel_mode(self):
        # -> schedule data_structure should be organized by travel mode
        pass
