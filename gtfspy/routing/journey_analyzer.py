from gtfspy.gtfs import GTFS
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTimeBoardingsAndRoute
from gtfspy.routing.models import Connection


class JourneyAnalyzer:
    def __init__(self,
                 gtfs,
                 stop_profiles,
                 target_stops):
        """

        :param gtfs: GTFS object
        :param stop_profiles: dict of NodeProfileMultiObjective
        :param target_stops: list
        """
        assert(isinstance(gtfs, GTFS))

        self.gtfs = gtfs
        self.stop_profiles = stop_profiles
        self.journey_dict = {}
        self.target_stops = target_stops
        self.materialize_journey()

    def materialize_journey(self):
        """
        This method extracts the data required from Connection and LabelTimeBoardingsAndRoute objects that are stored in
        NodeProfileMultiObjective objects.
        :return: list of dicts
        """

        for stop, stop_profile in self.stop_profiles.items():
            assert (isinstance(stop_profile, NodeProfileMultiObjective))

            stop_journeys = []
            for label in stop_profile.get_final_optimal_labels():
                assert (isinstance(label, LabelTimeBoardingsAndRoute))
                cur_label = label
                journey = _Journey(self.gtfs)
                journey_legs = []
                while True:
                    connection = cur_label.connection
                    if isinstance(connection, Connection):
                        journey_legs.append(connection)
                    if not cur_label.previous_label:
                        break
                    cur_label = cur_label.previous_label
                route_tuples = [(x.departure_stop, x.arrival_stop) for x in journey_legs]
                print(route_tuples)
                """
                while cur_label.previous_label:
                    connection = cur_label.connection
                    if isinstance(connection, Connection):
                        journey.journey_legs.append(connection)
                    cur_label = cur_label.previous_label

                stop_journeys.append(journey)
        self.journey_dict[stop] = stop_journeys
"""
    def calculate_passing_journeys_per_stop(self):
        """

        :return:
        """
        pass

    def calculate_passing_journeys_per_section(self):
        """

        :return:
        """
        pass

    def n_journey_alternatives(self):
        """
        Calculates the
        :return:
        """
        pass

    def n_departure_stop_alternatives(self):
        """

        :return:
        """
        pass

    def aggregate_in_vehicle_times(self, per_mode):
        pass

    def aggregate_in_vehicle_distances(self, per_mode):
        pass

    def aggregate_walking_times(self):
        pass

    def aggregate_walking_distance(self):
        pass

    def get_all_stop_sequences(self):
        all_stop_sequences = {}
        for stop, journeys in self.journey_dict.items():
            all_stop_sequences[stop] = [x.get_stop_sequence() for x in journeys]
        return all_stop_sequences


class _Journey:
    def __init__(self, gtfs=None):
        """
        This handles individual journeys
        :param journey_legs: list of connection objects
        :param gtfs: gtfs object
        """
        self.journey_legs = []
        self.gtfs = gtfs

    def add_leg(self, leg):
        self.journey_legs.append(leg)

    def get_journey_distance(self):
        pass

    def get_journey_time(self):
        """
        (using the connection objects)
        :return:
        """
        pass

    def get_journey_time_per_mode(self, modes=None):
        """

        :param modes: return these
        :return:
        """
        pass

    def get_walking_time(self):
        pass

    def get_stop_sequence(self):
        if self.journey_legs:
            stop_sequence = [self.journey_legs[0].departure_stop]
            for leg in self.journey_legs:
                stop_sequence.append(leg.arrival_stop)
            return stop_sequence
