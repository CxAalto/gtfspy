# from geojson import LineString, Feature, FeatureCollection
from geopandas import GeoDataFrame
from shapely.geometry import LineString
from pandas import DataFrame
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
        self.df = None
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
        print("Materializing journeys")
        journey_id = 0
        for stop, stop_profile in self.stop_profiles.items():
            assert (isinstance(stop_profile, NodeProfileMultiObjective))

            stop_journeys = []
            for label in stop_profile.get_final_optimal_labels():
                assert (isinstance(label, LabelTimeBoardingsAndRoute))
                journey = _Journey(self.gtfs, label, stop, journey_id)
                journey_id += 1
                stop_journeys.append(journey)
            self.journey_dict[stop] = stop_journeys

    def journeys_to_pandas(self):
        pass

    def _journey_legs_to_pandas(self):
        print("Transforming journey legs to pandas")
        leg_list = []
        for stop, journeys in self.journey_dict.items():
            for journey in journeys:
                for connection in journey.legs:
                    leg_list.append(connection.__dict__)
        self.df = DataFrame(leg_list)


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
            all_stop_sequences[stop] = [x.stop_sequence for x in journeys]
        return all_stop_sequences

    def get_all_geoms(self):
        all_geoms = []
        for stop, journeys in self.journey_dict.items():
            for journey in journeys:
                all_geoms.append(journey.coordinates)
        return all_geoms

    def get_section_counts(self):
        if not self.df:
            self._journey_legs_to_pandas()
        print("Producing section counts")
        df = self.df
        df_grouped = df.groupby(by=['departure_coordinate', 'arrival_coordinate', "departure_stop", "arrival_stop", "mode"]).agg(['count'])

        geometry = [LineString(x) for x in zip(df_grouped.departure_coordinate, df_grouped.arrival_coordinate)]
        df_grouped = df_grouped.drop(['departure_coordinate', 'arrival_coordinate'], axis=1)
        crs = {'init': 'epsg:4326'}
        geo_df = GeoDataFrame(df_grouped, crs=crs, geometry=geometry)
        return geo_df

    """
    def extract_geojson(self, geoms, attribute_data=None):
        ""
        Extracts geojson format from two matching lists of geometry and attribute data
        :param geoms: list of coordinate tuples
        :param attribute_data: list of dict
        :return:
        ""
        print("Extracting geojson")
        all_features = []
        if not attribute_data:
            attribute_data = [{}]*len(geoms)
        for geom, attribute in zip(geoms, attribute_data):
            linestring = LineString(geom)
            feature = Feature(geometry=linestring, properties=attribute)
            all_features.append(feature)
        features = FeatureCollection(all_features)
        return features
"""

class _Journey:
    def __init__(self, gtfs, label, origin_stop, journey_id):
        """
        This handles individual journeys
        :param gtfs: gtfs object
        :param label: label of the departure stop
        """
        # TODO: Find out how to determine if this is a fastest path or a less boardings path
        # TODO: Transfer stops
        # TODO: circuity/directness
        self.legs = []
        self.gtfs = gtfs
        self.label_dep_time = label.departure_time
        self.label_arr_time = label.arrival_time_target
        self.label_n_boardings = label.n_boardings
        self.origin_stop = origin_stop
        self.journey_id = journey_id
        """
        journey_legs contain the following data for each leg (as a dict):
        departure_stop
        arrival_stop
        departure_time
        arrival_time
        trip_id
        is_walk

        the following values can be generated:
        departure_coordinate
        arrival_coordinate
        mode
        duration
        distance
        """
        cur_label = label
        while True:
            connection = cur_label.connection
            if isinstance(connection, Connection):
                self.legs.append(connection)
            if not cur_label.previous_label:
                break
            cur_label = cur_label.previous_label

        self._assign_optional_connection_variables()
        self.stop_sequence = []  # list of stop_I of all visited nodes
        self._calculate_stop_sequence()
        self.coordinates = []  # list of (lat, lon)
        self._calculate_coordinate_sequence()

    def _assign_optional_connection_variables(self):
        for connection in self.legs:
            lat, lon = self.gtfs.get_stop_coordinates(connection.departure_stop)
            connection.departure_coordinate = (lon, lat)
            lat, lon = self.gtfs.get_stop_coordinates(connection.arrival_stop)
            connection.arrival_coordinate = (lon, lat)
            if connection.trip_id:
                connection.route_name, connection.mode = self.gtfs.get_route_name_and_type_of_tripI(connection.trip_id)
            else:
                connection.route_name = None
                connection.mode = 'walking'

    def get_section_coordinates(self):
        section_coordinates = []
        for connection in self.legs:
            section_coordinates.append([connection.departure_coordinate, connection.arrival_coordinate])
        return section_coordinates

    def get_section_metadata(self):
        section_metadata = []
        for connection in self.legs:
            section_metadata.append(
                {"mode": connection.mode,
                 "route_name": connection.route_name}
            )
        return section_metadata

    def get_transfer_stops_and_sections(self):
        pass

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

    def _calculate_stop_sequence(self):
        if self.legs:
            self.stop_sequence = [self.legs[0].departure_stop]
            for leg in self.legs:
                self.stop_sequence.append(leg.arrival_stop)

    def _calculate_coordinate_sequence(self):
        for stop in self.stop_sequence:
            lat, lon = self.gtfs.get_stop_coordinates(stop)
            self.coordinates.append((lon, lat))

