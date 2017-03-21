# from geojson import LineString, Feature, FeatureCollection
import sqlite3
import os
from geopandas import GeoDataFrame
from shapely.geometry import LineString
from pandas import DataFrame
from gtfspy.gtfs import GTFS
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTimeBoardingsAndRoute
from gtfspy.routing.models import Connection
from gtfspy.util import timeit

# TODO: store journey data in sqlite DB
# TODO: DB-handling: make sure the connection is closed using __enter__, __exit__?
# TODO: Transfer stops
# TODO: circuity/directness


class JourneyAnalyzer:
    @timeit
    def __init__(self,
                 fname,
                 gtfs,
                 stop_profiles,
                 target_stops,
                 gtfs_dir
                 ):
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
        self.gtfs_dir = gtfs_dir
        self.conn = sqlite3.connect(fname)



        # TODO: Some kind of info table, so that:
        # - we can retain a connection to the source data
        # - date, city, parameters etc information
        self._populate_connection_table_with_gtfs_data()
        self.insert_journeys_into_db()
        self.conn.commit()

    def import_journey_data(self):

    def set_up_tables(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS journeys(
                     journey_id INTEGER PRIMARY KEY,
                     from_id INT,
                     to_id INT,
                     dep_time INT,
                     arr_time INT,
                     n_boardings INT,
                     from_coord REAL,
                     to_coord REAL)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS connections(
                     journey_id INT,
                     from_id INT,
                     to_id INT,
                     dep_time INT,
                     arr_time INT,
                     from_lat REAL,
                     from_lon REAL,
                     to_lat REAL,
                     to_lon REAL,
                     trip_id INT,
                     mode INT,
                     route_name TEXT,
                     seq INT)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS walk_durations(
                     from_id INT,
                     to_id INT,
                     duration REAL)''')


    def insert_journeys_into_db(self):
        print("Materializing journeys")
        journey_id = 0  # TODO: this initial journey_id should depend on the last id already in the table

        # TODO: Find out how to determine if this is a fastest path or a less boardings path
        tot = len(self.stop_profiles)
        for i, origin_stop in enumerate(self.stop_profiles, start= 1):
            print("\r Stop " + str(i) + " of " + str(tot), end='', flush=True)
            assert (isinstance(self.stop_profiles[origin_stop], NodeProfileMultiObjective))

            value_list = []
            for label in self.stop_profiles[origin_stop].get_final_optimal_labels():
                assert (isinstance(label, LabelTimeBoardingsAndRoute))
                # We need to "unpack" the journey to actually figure out where the trip went
                # (there can be several targets).

                target_stop = self._insert_connections_into_db(journey_id, label)
                values = [journey_id,
                          origin_stop,
                          target_stop,
                          int(label.departure_time),
                          int(label.arrival_time_target),
                          label.n_boardings]

                stmt = '''INSERT INTO journeys(
                      journey_id,
                      from_id,
                      to_id,
                      dep_time,
                      arr_time,
                      n_boardings) VALUES (%s) ''' % (", ".join(["?" for x in values]))
                value_list.append(values)
                journey_id += 1

            self.conn.executemany(stmt, value_list)
        print()

    def _insert_connections_into_db(self, journey_id, label):
        cur_label = label
        seq = 1
        value_list = []
        while True:
            connection = cur_label.connection
            if isinstance(connection, Connection):
                if connection.trip_id:
                    trip_id = connection.trip_id
                else:
                    trip_id = -1
                values = (
                    int(journey_id),
                    int(connection.departure_stop),
                    int(connection.arrival_stop),
                    int(connection.departure_time),
                    int(connection.arrival_time),
                    #from_lat,
                    #from_lon,
                    #to_lat,
                    #to_lon,
                    int(trip_id),
                    #mode,
                    #route_name,
                    int(seq)
                )
                stmt = '''INSERT INTO connections(
                                      journey_id,
                                      from_id,
                                      to_id,
                                      dep_time,
                                      arr_time,
                                      trip_id,
                                      seq) VALUES (%s) ''' % (", ".join(["?" for x in values]))
                value_list.append(values)
                seq += 1
                target_stop = connection.arrival_stop
            if not cur_label.previous_label:
                break
            cur_label = cur_label.previous_label

        self.conn.executemany(stmt, value_list)

        return target_stop

    def _populate_connection_table_with_gtfs_data(self):
        cur = self.conn.cursor()
        print(self.gtfs_dir)
        cur.execute("ATTACH '%s' as 'gtfs'" % str(self.gtfs_dir))
        cur.execute("PRAGMA database_list")
        print(cur.fetchone())
        # route_name, mode = self.gtfs.get_route_name_and_type_of_tripI(connection.trip_id)
        # from_lat, from_lon = self.gtfs.get_stop_coordinates(connection.departure_stop)
        # to_lat, to_lon = self.gtfs.get_stop_coordinates(connection.arrival_stop)

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



    """    
                tabledefs = {
            "journeys":
                [
                    ("journey_id", "INT"),
                    ("from_id", "INT"),
                    ("to_id", "INT"),
                    ("dep_time", "INT"),
                    ("arr_time", "INT"),
                    ("n_transfers", "INT"),
                    ("from_coord", "REAL"),
                    ("to_coord", "REAL")
                ],
            "connections":
                [
                    ("journey_id", "INT"),
                    ("from_id", "INT"),
                    ("to_id", "INT"),
                    ("dep_time", "INT"),
                    ("arr_time", "INT"),
                    ("from_coord", "REAL"),
                    ("to_coord", "REAL"),
                    ("mode", "INT"),
                    ("route_id", "INT"),
                    ("seq", "INT")
                ],
            "walk_durations":
                [
                    ("from_id", "INT"),
                    ("to_id", "INT"),
                    ("duration", "REAL")
                ]
        }
"""


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



