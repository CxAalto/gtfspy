import sqlite3
import os
from pandas import DataFrame, read_sql_query
from shapely.geometry import Point, LineString
import matplotlib.pyplot as plt
from geopandas import GeoDataFrame
from gtfspy.gtfs import GTFS

from gtfspy.util import timeit

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
    @timeit
    def calculate_passing_journeys_per_section(self, fastest_path=False, time_weighted=False):
        """

        :return:
        """
        query = 'SELECT connections.from_stop_I AS from_stop_I, connections.to_stop_I AS to_stop_I, ' \
                'from_.lat AS from_lat, from_.lon AS from_lon, ' \
                'to_.lat AS to_lat, to_.lon AS to_lon, count(*) AS n_trip ' \
                'FROM connections, ' \
                '(SELECT stop_I, lat, lon  FROM gtfs.stops) from_, ' \
                '(SELECT stop_I, lat, lon  FROM gtfs.stops) to_ ' \
                'WHERE from_.stop_I = connections.from_stop_I AND to_.stop_I = connections.to_stop_I ' \
                'GROUP BY connections.from_stop_I, connections.to_stop_I'

        if time_weighted:
            raise NotImplementedError
        if fastest_path:
            query = ', journeys WHERE connections.journey_id = journeys.journey_id AND journeys.fastest_path = 1 '
            raise NotImplementedError

        df = read_sql_query(query, self.conn)

        df['geometry'] = df.apply(lambda x: LineString([(x.from_lon, x.from_lat), (x.to_lon, x.to_lat)]), axis=1)
        gdf = GeoDataFrame(df, geometry='geometry')
        gdf.crs = {'init': 'epsg:4326'}
        gdf.plot()
        plt.show()

    def to_geopandas(self):
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

    @timeit
    def get_transfer_stops(self, group_by_routes=False):
        query = 'SELECT to_stop_I AS stop_I, lat, lon, count(*) AS n_trips  FROM ' \
                '(SELECT journey_id, to_stop_I, trip_I FROM connections) c1, ' \
                '(SELECT journey_id, from_stop_I, trip_I FROM connections) c2, ' \
                '(SELECT stop_I, lat, lon FROM gtfs.stops) gtfs ' \
                'WHERE c1.journey_id=c2.journey_id AND c1.to_stop_I=c2.from_stop_I ' \
                'AND c1.trip_I != c2.trip_I AND c1.to_stop_I=gtfs.stop_I ' \
                'GROUP BY stop_I'

        df = read_sql_query(query, self.conn)

        print(df)
        df['geometry'] = df.apply(lambda x: Point((x.lon, x.lat)), axis=1)
        gdf = GeoDataFrame(df, geometry='geometry')
        gdf.crs = {'init': 'epsg:4326'}
        return gdf
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

