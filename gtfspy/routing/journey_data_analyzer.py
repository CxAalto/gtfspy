import sqlite3
import os
from gtfspy.gtfs import GTFS

class JourneyDataAnalyzer:
    # TODO: Transfer stops
    # TODO: circuity/directness

    def __init__(self, journey_db_dir, gtfs_dir):
        assert os.path.isfile(journey_db_dir)
        assert os.path.isfile(gtfs_dir)
        self.conn = sqlite3.connect(journey_db_dir)
        self._attach_gtfs_database(gtfs_dir)

    def __del__(self):
        self.conn.close()

    def _attach_gtfs_database(self, gtfs_dir):
        cur = self.conn.cursor()
        cur.execute("ATTACH '%s' as 'gtfs'" % str(gtfs_dir))
        cur.execute("PRAGMA database_list")
        print("GTFS database attached:", cur.fetchall())

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

