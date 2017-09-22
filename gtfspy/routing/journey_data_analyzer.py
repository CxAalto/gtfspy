import os
import sqlite3
import numpy as np

from pandas import read_sql_query, DataFrame, Series
from gtfspy.gtfs import GTFS
from gtfspy.util import timeit
from gtfspy.routing.journey_data import attach_database

class JourneyDataAnalyzer:
    # TODO: Transfer stops
    # TODO: circuity/directness

    def __init__(self, journey_db_path, gtfs_path):
        assert os.path.isfile(journey_db_path)
        assert os.path.isfile(gtfs_path)
        self.conn = sqlite3.connect(journey_db_path)
        self.g = GTFS(gtfs_path)
        self.gtfs_path = gtfs_path
        self.conn = attach_database(self.conn, self.gtfs_path)

    def __del__(self):
        self.conn.close()

    def get_journey_legs_to_target(self, target, fastest_path=True, min_boardings=False, all_leg_sections=True,
                                                   ignore_walk=False, diff_threshold=None, diff_path=None):
        """
        Returns a dataframe of aggregated sections from source nodes to target. The returned sections are either
        transfer point to transfer point or stop to stop. In a before after setting, the results can be filtered based
        on values in a difference db.
        :param target:
        :param fastest_path:
        :param min_boardings:
        :param all_leg_sections:
        :param ignore_walk:
        :param diff_threshold:
        :param diff_path:
        :return:
        """
        assert not (fastest_path and min_boardings)
        if min_boardings:
            raise NotImplementedError
        if all_leg_sections and diff_threshold:
            raise NotImplementedError

        added_constraints = ""
        add_diff = ""
        if fastest_path:
            added_constraints += " AND journeys.pre_journey_wait_fp>=0"
        if ignore_walk:
            added_constraints += " AND legs.trip_I >= 0"
        if diff_path and diff_threshold:
            self.conn = attach_database(self.conn, diff_path, name="diff")
            add_diff = ", diff.diff_temporal_distance"
            added_constraints += " AND abs(diff_temporal_distance.diff_mean) >= %s " \
                                 "AND diff_temporal_distance.from_stop_I = journeys.from_stop_I " \
                                 "AND diff_temporal_distance.to_stop_I = journeys.to_stop_I" % (diff_threshold,)

        if all_leg_sections:
            df = self._get_journey_legs_to_target_with_all_sections(target, added_constraints)
        else:
            query = """SELECT from_stop_I, to_stop_I, coalesce(type, -1) AS type,
                         count(*) AS n_trips
                         FROM
                         (SELECT legs.* FROM legs, journeys %s
                         WHERE journeys.journey_id = legs.journey_id AND journeys.to_stop_I = %s %s) q1
                         LEFT JOIN (SELECT * FROM other.trips, other.routes WHERE trips.route_I = routes.route_I) q2
                         ON q1.trip_I = q2.trip_I
                         GROUP BY from_stop_I, to_stop_I, type""" % (add_diff, str(target), added_constraints)
            df = read_sql_query(query, self.conn)

        return df

    def _get_journey_legs_to_target_with_all_sections(self, target, added_constraint):
        def gen_pairs(stop_lists):
            for stop_list in stop_lists:
                prev_stop = None
                stop_pair_list = []
                for stop in stop_list:
                    if prev_stop:
                        stop_pair_list.append((int(prev_stop), int(stop)))
                    prev_stop = stop
                yield stop_pair_list

        query = """SELECT leg_stops, coalesce(type, -1) AS type, count(*) AS n_trips FROM
                   (SELECT legs.* FROM legs, journeys
                   WHERE journeys.journey_id = legs.journey_id AND journeys.to_stop_I = %s %s) q1
                     LEFT JOIN (SELECT * FROM other.trips, other.routes WHERE trips.route_I = routes.route_I) q2
                     ON q1.trip_I = q2.trip_I
                     GROUP BY leg_stops, type""" % (str(target), added_constraint)
        orig_df = read_sql_query(query, self.conn)

        df = DataFrame([x for x in gen_pairs(orig_df.leg_stops.str.split(',').tolist())],
                          index=[orig_df.type, orig_df.n_trips]).stack()

        df = df.reset_index()
        df = df.rename(columns={0: "stop_tuple"})
        df[['from_stop_I', 'to_stop_I']] = df['stop_tuple'].apply(Series)

        df = df.groupby(['from_stop_I', 'to_stop_I', 'type']).agg({'n_trips': [np.sum]})
        df = df.reset_index()
        df.columns = df.columns.droplevel(1)
        df_to_return = df[['from_stop_I', 'to_stop_I', 'type', 'n_trips']]

        return df_to_return

    def get_origin_target_journey_legs(self, origin, target, start_time=None, end_time=None, fastest_path=True, min_boardings=False,
                                       ignore_walk=False, add_coordinates=True):

        assert not (fastest_path and min_boardings)
        if min_boardings:
            raise NotImplementedError

        added_constraints = ""
        if fastest_path:
            added_constraints += " AND journeys.pre_journey_wait_fp>=0"
        if ignore_walk:
            added_constraints += " AND legs.trip_I >= 0"
        if start_time:
            added_constraints += " AND journeys.departure_time>= %s" % start_time
        if start_time:
            added_constraints += " AND journeys.departure_time< %s" % end_time

        query = """SELECT from_stop_I, to_stop_I, coalesce(type, -1) AS type,
                     count(*) AS n_trips, group_concat(dep_time) AS dep_times
                     FROM
                     (SELECT legs.*, journeys.departure_time as dep_time FROM legs, journeys
                     WHERE journeys.journey_id = legs.journey_id AND journeys.from_stop_I = %s 
                     AND journeys.to_stop_I = %s %s
                     ORDER BY dep_time
                     ) q1
                     LEFT JOIN (SELECT * FROM other.trips, other.routes WHERE trips.route_I = routes.route_I) q2
                     ON q1.trip_I = q2.trip_I
                     GROUP BY from_stop_I, to_stop_I, type""" % (str(origin), str(target), added_constraints)
        df = read_sql_query(query, self.conn)
        if add_coordinates:
            df = self.g.add_coordinates_to_df(df, join_column="from_stop_I", lat_name="from_lat", lon_name="from_lon")
            df = self.g.add_coordinates_to_df(df, join_column="to_stop_I", lat_name="to_lat", lon_name="to_lon")
        return df

    def get_journey_routes_not_in_other_db(self, target, other_journey_conn, fastest_path=True, min_boardings=False, all_leg_sections=True,
                                           ignore_walk=False, diff_threshold=None, diff_path=None):
        name = "ojdb"
        added_constraints = ""
        if fastest_path:
            added_constraints += " AND journeys.pre_journey_wait_fp>=0"
        if ignore_walk:
            added_constraints += " AND legs.trip_I >= 0"

        query = """SELECT from_stop_I, to_stop_I, coalesce(type, -1) AS type, route FROM 
                    (SELECT legs.*, route FROM journeys, legs WHERE legs.journey_id=journeys.journey_id AND journeys.to_stop_I = %s %s) q1
                    LEFT JOIN 
                    (SELECT * FROM other.trips, other.routes WHERE trips.route_I = routes.route_I) q2
                    ON q1.trip_I = q2.trip_I
                    """ % (str(target), added_constraints)

        df = read_sql_query(query, self.conn)

        routes = other_journey_conn.execute("SELECT DISTINCT route FROM journeys WHERE to_stop_I = %s" % (str(target),)).fetchall()
        routes = [x[0] for x in routes]
        other_set = set(routes)
        these_routes = self.conn.execute("SELECT DISTINCT route FROM journeys WHERE to_stop_I = %s" % (str(target),)).fetchall()
        these_routes = [x[0] for x in these_routes]
        this_set = set(these_routes)
        print("n unique routes for this db: ", len(this_set))
        print("n unique routes for other db: ", len(other_set))

        union = other_set | this_set
        intersection = other_set & this_set
        print("n unique routes", len(union))
        print("n common routes", len(intersection))

        df = df.loc[~df['route'].isin(routes)]
        df = df[["from_stop_I", "to_stop_I", "type"]]
        df = DataFrame({"n_trips": df.groupby(["from_stop_I", "to_stop_I", "type"]).size()}).reset_index()
        return df

    def journey_alternatives_per_stop_pair(self, target, start_time, end_time):
        query = """SELECT from_stop_I, to_stop_I, ifnull(1.0*sum(n_sq)/(sum(n_trips)*(sum(n_trips)-1)), 1) AS simpson,
                    sum(n_trips) AS n_trips, count(*) AS n_routes FROM 
                    (SELECT from_stop_I, to_stop_I, count(*) AS n_trips, count(*)*(count(*)-1) AS n_sq 
                    FROM journeys
                    WHERE pre_journey_wait_fp > 0 AND to_stop_I = %s AND departure_time >= %s AND departure_time <= %s
                    GROUP BY route) sq1
                    GROUP BY from_stop_I, to_stop_I""" % (target, start_time, end_time)
        df = read_sql_query(query, self.conn)
        df = self.g.add_coordinates_to_df(df, join_column="from_stop_I")

        return df

    def journey_alternative_data_time_weighted(self, target, start_time, end_time):
        query = """SELECT sum(p*p) AS simpson, sum(n_trips) AS n_trips, count(*) AS n_routes, from_stop_I, to_stop_I FROM
                    (SELECT 1.0*sum(pre_journey_wait_fp)/total_time AS p, count(*) AS n_trips, route, 
                    journeys.from_stop_I, journeys.to_stop_I FROM journeys,
                    (SELECT sum(pre_journey_wait_fp) AS total_time, from_stop_I, to_stop_I FROM journeys
                    WHERE departure_time >= %s AND departure_time <= %s
                    GROUP BY from_stop_I, to_stop_I) sq1
                    WHERE pre_journey_wait_fp > 0 AND sq1.to_stop_I=journeys.to_stop_I AND departure_time >= %s 
                    AND departure_time <= %s AND journeys.to_stop_I = %s AND sq1.from_stop_I = journeys.from_stop_I 
                    GROUP BY route) sq2
                    GROUP BY from_stop_I, to_stop_I""" % (start_time, end_time, start_time, end_time, target)
        df = read_sql_query(query, self.conn)
        df = self.g.add_coordinates_to_df(df, join_column="from_stop_I")

        return df

    def _add_to_from_coordinates_to_df(self, df):
        df = self.g.add_coordinates_to_df(df, join_column="from_stop_I", lat_name="from_lat", lon_name="from_lon")
        df = self.g.add_coordinates_to_df(df, join_column="to_stop_I", lat_name="to_lat", lon_name="to_lon")
        return df

    def get_upstream_stops(self, target, stop):
        query = """SELECT stops.* FROM other.stops, 
                    (SELECT journeys.from_stop_I AS stop_I FROM journeys, legs 
                    WHERE journeys.journey_id=legs.journey_id AND legs.from_stop_I = %s AND journeys.to_stop_I = %s AND pre_journey_wait_fp >= 0
                    GROUP BY journeys.from_stop_I) q1
                    WHERE stops.stop_I = q1.stop_I""" % (stop, target)
        df = read_sql_query(query, self.conn)
        return df

    def get_upstream_stops_ratio(self, target, stops, ratio):
        if isinstance(stops, list):
            stops = ",".join(stops)
        query = """SELECT stops.* FROM other.stops, 
                    (SELECT q2.from_stop_I AS stop_I FROM 
                    (SELECT journeys.from_stop_I, count(*) AS n_total FROM journeys
                    WHERE journeys.to_stop_I = %s
                    GROUP BY from_stop_I) q1,
                    (SELECT journeys.from_stop_I, count(*) AS n_trough FROM journeys, legs 
                    WHERE journeys.journey_id=legs.journey_id AND legs.from_stop_I IN (%s) AND journeys.to_stop_I = %s
                    GROUP BY journeys.from_stop_I) q2
                    WHERE q1.from_stop_I = q2.from_stop_I AND n_trough/(n_total*1.0) >= %s) q1
                    WHERE stops.stop_I = q1.stop_I""" % (target, stops, target, ratio)
        df = read_sql_query(query, self.conn)
        return df

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
        pass

    @timeit
    def get_transfer_walks(self, group_by_routes=False):
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

