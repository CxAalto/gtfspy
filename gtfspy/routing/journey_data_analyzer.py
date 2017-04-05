import os
import sqlite3

from geopandas import GeoDataFrame
from pandas import read_sql_query
from shapely.geometry import Point, LineString

from gtfspy.util import timeit


class JourneyDataAnalyzer:
    # TODO: Transfer stops
    # TODO: circuity/directness

    def __init__(self, journey_db_dir, gtfs_dir):
        assert os.path.isfile(journey_db_dir)
        assert os.path.isfile(gtfs_dir)
        self.conn = sqlite3.connect(journey_db_dir)
        self._attach_gtfs_database(gtfs_dir)
        self._create_temporary_views()

    def __del__(self):
        self.conn.close()

    def _attach_gtfs_database(self, gtfs_dir):
        cur = self.conn.cursor()
        cur.execute("ATTACH '%s' as 'gtfs'" % str(gtfs_dir))
        cur.execute("PRAGMA database_list")
        print("GTFS database attached:", cur.fetchall())

    def _create_temporary_views(self):
        # could use self_or_parent_I for stop_I?
        self.conn.execute('''CREATE TEMP VIEW IF NOT EXISTS extended_connections(journey_id,
                     o_stop_I,
                     d_stop_I,
                     from_stop_I,
                     to_stop_I,
                     dep_time,
                     arr_time,
                     trip_I,
                     seq,
                     route,
                     fastest_path,
                     from_lat,
                     from_lon,
                     to_lat,
                     to_lon,
                     o_lat,
                     o_lon,
                     d_lat,
                     d_lon,
                     route_name,
                     mode)
                     AS
                     SELECT
                     c.journey_id AS journey_id,
                     j.from_stop_I AS o_stop_I,
                     j.to_stop_I AS d_stop_I,
                     c.from_stop_I AS from_stop_I,
                     c.to_stop_I AS to_stop_I,
                     c.dep_time,
                     c.arr_time,
                     c.trip_I,
                     c.seq,
                     j.route,
                     j.fastest_path,
                     from_.lat AS from_lat,
                     from_.lon AS from_lon,
                     to_.lat AS to_lat,
                     to_.lon AS to_lon,
                     o_.lat AS o_lat,
                     o_.lon AS o_lon,
                     d_.lat AS d_lat,
                     d_.lon AS d_lon,
                     CASE WHEN c.trip_I = -1 THEN 'walk' ELSE name END AS route_name,
                     CASE WHEN c.trip_I = -1 THEN -1 ELSE type END AS mode
                     FROM
                     (SELECT * FROM connections) c
                     LEFT JOIN
                     (SELECT trip_I, trips.route_I, name, type
                     FROM gtfs.trips, gtfs.routes
                     WHERE trips.route_I=routes.route_I) r ON c.trip_I = r.trip_I,
                     (SELECT journey_id, from_stop_I, to_stop_I, route, fastest_path FROM journeys) j,
                     (SELECT stop_I, lat, lon FROM gtfs.stops) from_,
                     (SELECT stop_I, lat, lon FROM gtfs.stops) to_,
                     (SELECT stop_I, lat, lon FROM gtfs.stops) o_,
                     (SELECT stop_I, lat, lon FROM gtfs.stops) d_
                     WHERE c.journey_id = j.journey_id
                     AND c.from_stop_I = from_.stop_I AND c.to_stop_I = to_.stop_I
                     AND j.from_stop_I = o_.stop_I AND j.to_stop_I = d_.stop_I
                     ''')

        self.conn.execute('''CREATE TEMP VIEW IF NOT EXISTS extended_journeys(journey_id,
                     dep_time,
                     arr_time,
                     o_stop_I,
                     d_stop_I,
                     route,
                     fastest_path,
                     o_lat,
                     o_lon,
                     d_lat,
                     d_lon)
                     AS
                     SELECT
                     j.journey_id AS journey_id,
                     j.dep_time,
                     j.arr_time,
                     j.from_stop_I AS o_stop_I,
                     j.to_stop_I AS d_stop_I,
                     j.route,
                     j.fastest_path,
                     o_.lat AS o_lat,
                     o_.lon AS o_lon,
                     d_.lat AS d_lat,
                     d_.lon AS d_lon
                     FROM
                     (SELECT journey_id, dep_time, arr_time, from_stop_I, to_stop_I, route, fastest_path FROM journeys) j,
                     (SELECT stop_I, lat, lon FROM gtfs.stops) o_,
                     (SELECT stop_I, lat, lon FROM gtfs.stops) d_
                     WHERE
                     j.from_stop_I = o_.stop_I AND j.to_stop_I = d_.stop_I
                     ''')

    def journey_alternatives_per_stop(self, target=None):
        query = 'SELECT o_stop_I AS stop_I, o_lat AS lat, o_lon AS lon, count(*) AS n_journeys FROM extended_journeys ' \
                'GROUP BY o_stop_I'
        df = read_sql_query(query, self.conn)

        df['geometry'] = df.apply(lambda x: Point((x.lon, x.lat)), axis=1)
        gdf = GeoDataFrame(df, geometry='geometry')
        gdf.crs = {'init': 'epsg:4326'}
        return gdf

    def passing_journeys_per_stop(self):
        """

        :return:
        """
        pass
    @timeit
    def journeys_per_section(self, fastest_path=False, time_weighted=False):
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
        #gdf.plot()
        #plt.show()
        return gdf

    def to_geopandas(self):
        pass

    @timeit
    def n_route_alternatives(self):
        """
        Calculates the
        :return:
        """
        query = "SELECT *, count(*) FROM " \
                "(SELECT o_stop_I, d_stop_I, group_concat(route_name) AS route FROM " \
                "(SELECT * FROM extended_connections WHERE fastest_path = 1 ORDER BY journey_id, seq) subquery " \
                "GROUP BY journey_id) subquery2 " \
                "GROUP BY route " \
                "ORDER BY o_stop_I"

        query = 'SELECT *, count(*) AS n_trips  FROM ' \
                '(SELECT c1.journey_id, o_stop_I, d_stop_I, group_concat(to_stop_I) AS route FROM ' \
                '(SELECT journey_id, to_stop_I, trip_I, o_stop_I, d_stop_I FROM extended_connections) c1, ' \
                '(SELECT journey_id, from_stop_I, trip_I FROM extended_connections) c2 ' \
                'WHERE c1.journey_id=c2.journey_id AND c1.to_stop_I=c2.from_stop_I ' \
                'AND c1.trip_I != c2.trip_I ' \
                'GROUP BY c1.journey_id) sq1 ' \
                'GROUP BY o_stop_I, d_stop_I, route'

        query = 'SELECT o_stop_I, d_stop_I, route, count(*) from extended_journeys ' \
                'GROUP BY o_stop_I, d_stop_I, route'

        df = read_sql_query(query, self.conn)
        print(df.to_string)
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
        query = 'SELECT to_stop_I AS stop_I, lat, lon, count(*) AS n_journeys  FROM ' \
                '(SELECT journey_id, to_stop_I, trip_I FROM connections) c1, ' \
                '(SELECT journey_id, from_stop_I, trip_I FROM connections) c2, ' \
                '(SELECT stop_I, lat, lon FROM gtfs.stops) gtfs ' \
                'WHERE c1.journey_id=c2.journey_id AND c1.to_stop_I=c2.from_stop_I ' \
                'AND c1.trip_I != c2.trip_I AND c1.to_stop_I=gtfs.stop_I AND c1.trip_I != -1 AND c2.trip_I != -1 ' \
                'GROUP BY to_stop_I'

        df = read_sql_query(query, self.conn)

        df['geometry'] = df.apply(lambda x: Point((x.lon, x.lat)), axis=1)
        gdf = GeoDataFrame(df, geometry='geometry')
        gdf.crs = {'init': 'epsg:4326'}
        return gdf

    @timeit
    def get_transfer_walks(self, group_by_routes=False):
        query = 'SELECT c2.from_stop_I AS from_stop_I, c2.to_stop_I AS to_stop_I, ' \
                'gtfs1.lat AS from_lat, gtfs1.lon AS from_lon, gtfs2.lat AS to_lat, gtfs2.lon AS to_lon, ' \
                'count(*) AS n_journeys  ' \
                'FROM ' \
                '(SELECT journey_id, to_stop_I, trip_I FROM connections) c1, ' \
                '(SELECT journey_id, from_stop_I, to_stop_I, trip_I FROM connections) c2, ' \
                '(SELECT journey_id, from_stop_I, trip_I FROM connections) c3, ' \
                '(SELECT stop_I, lat, lon FROM gtfs.stops) gtfs1, ' \
                '(SELECT stop_I, lat, lon FROM gtfs.stops) gtfs2 ' \
                'WHERE c1.journey_id=c2.journey_id AND c2.journey_id=c3.journey_id ' \
                'AND c1.to_stop_I=c2.from_stop_I AND c2.to_stop_I=c3.from_stop_I ' \
                'AND c2.from_stop_I=gtfs1.stop_I AND c2.to_stop_I=gtfs2.stop_I ' \
                'AND c1.trip_I > -1 AND c2.trip_I = -1 AND c3.trip_I > -1 ' \
                'GROUP BY c2.from_stop_I, c2.to_stop_I'

        df = read_sql_query(query, self.conn)
        print(df)
        df['geometry'] = df.apply(lambda x: LineString([(x.from_lon, x.from_lat), (x.to_lon, x.to_lat)]), axis=1)
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

