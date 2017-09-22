import os
import sqlite3
import pandas as pd

from gtfspy.routing.connection import Connection
from gtfspy.gtfs import GTFS
from gtfspy.routing.label import LabelTimeAndRoute, LabelTimeWithBoardingsCount, LabelTimeBoardingsAndRoute, \
    compute_pareto_front, LabelGeneric
from gtfspy.routing.fastest_path_analyzer import FastestPathAnalyzer
from gtfspy.routing.node_profile_analyzer_time_and_veh_legs import NodeProfileAnalyzerTimeAndVehLegs
from gtfspy.util import timeit


def attach_database(conn, other_db_path, name="other"):
    cur = conn.cursor()

    cur.execute("ATTACH '%s' AS '%s'" % (str(other_db_path), name))
    cur.execute("PRAGMA database_list")
    print("other database attached:", cur.fetchall())
    return conn

_T_WALK_STR = "t_walk"

class JourneyDataManager:

    def __init__(self, gtfs_path, journey_db_path, routing_params=None, multitarget_routing=False,
                 track_vehicle_legs=True, track_route=False):
        """
        :param gtfs: GTFS object
        :param list_of_stop_profiles: dict of NodeProfileMultiObjective
        :param multitarget_routing: bool
        """
        self.multitarget_routing = multitarget_routing
        self.track_route = track_route
        self.track_vehicle_legs = track_vehicle_legs
        self.gtfs_path = gtfs_path
        self.gtfs = GTFS(self.gtfs_path)
        self.gtfs_meta = self.gtfs.meta
        self.gtfs._dont_close = True
        self.od_pairs = None
        self._targets = None
        self._origins = None
        self.diff_conn = None

        if not routing_params:
            routing_params = dict()
        self.routing_params_input = routing_params

        assert os.path.exists(journey_db_path) or routing_params is not None
        journey_db_pre_exists = os.path.isfile(journey_db_path)

        # insert a pretty robust timeout:
        timeout = 1000
        self.conn = sqlite3.connect(journey_db_path, timeout)
        if not journey_db_pre_exists:
            self.initialize_database()

        self.routing_parameters = Parameters(self.conn)
        self._assert_journey_computation_paramaters_match()

        self.journey_properties = {"journey_duration": (_T_WALK_STR, _T_WALK_STR)}
        if routing_params.get('track_vehicle_legs', False) or \
                self.routing_parameters.get('track_vehicle_legs', False):
            self.journey_properties["n_boardings"] = (float("inf"), 0)
        if self.track_route:
            additional_journey_parameters = {
                "in_vehicle_duration": (float('inf'), 0),
                "transfer_wait_duration": (float('inf'), 0),
                "walking_duration": (_T_WALK_STR, _T_WALK_STR),
                "pre_journey_wait_fp": (float('inf'), 0)
            }
            self.journey_properties.update(additional_journey_parameters)
        self.travel_impedance_measure_names = list(self.journey_properties.keys())
        self.travel_impedance_measure_names += ["temporal_distance"]

    def __del__(self):
        self.gtfs._dont_close = False
        if self.conn:
            self.conn.close()

    @timeit
    def import_journey_data_for_target_stop(self, target_stop_I, origin_stop_I_to_journey_labels):
        """
        Parameters
        ----------
        origin_stop_I_to_journey_labels: dict
            key: origin_stop_Is
            value: list of labels
        target_stop_I: int
        """
        cur = self.conn.cursor()
        self.conn.isolation_level = 'EXCLUSIVE'
        cur.execute('PRAGMA synchronous = OFF;')

        if self.track_route:
            self._insert_journeys_with_route_into_db(origin_stop_I_to_journey_labels, target_stop=int(target_stop_I))
        else:
            self._insert_journeys_into_db_no_route(origin_stop_I_to_journey_labels, target_stop=int(target_stop_I))
        print("Finished import process")

    def _assert_journey_computation_paramaters_match(self):
        for key, value in self.routing_parameters.items():
            if key in self.gtfs_meta.keys():
                assert self.gtfs_meta[key] == value

    def _get_largest_journey_id(self):
        cur = self.conn.cursor()
        val = cur.execute("select max(journey_id) FROM journeys").fetchone()
        return val[0] if val[0] else 0

    def _insert_journeys_into_db_no_route(self, stop_profiles, target_stop=None):
        # TODO: Change the insertion so that the check last journey id and insertions are in the same transaction block
        """
        con.isolation_level = 'EXCLUSIVE'
        con.execute('BEGIN EXCLUSIVE')
        #exclusive access starts here. Nothing else can r/w the db, do your magic here.
        con.commit()
        """
        print("Collecting journey data")
        journey_id = 1
        journey_list = []
        tot = len(stop_profiles)
        for i, (origin_stop, labels) in enumerate(stop_profiles.items(), start=1):
            #print("\r Stop " + str(i) + " of " + str(tot), end='', flush=True)
            for label in labels:
                assert (isinstance(label, LabelTimeWithBoardingsCount))
                if self.multitarget_routing:
                    target_stop = None
                else:
                    target_stop = int(target_stop)

                values = [int(journey_id),
                          int(origin_stop),
                          target_stop,
                          int(label.departure_time),
                          int(label.arrival_time_target),
                          int(label.n_boardings)]

                journey_list.append(values)
                journey_id += 1
        print("Inserting journeys into database")
        insert_journeys_stmt = '''INSERT INTO journeys(
              journey_id,
              from_stop_I,
              to_stop_I,
              departure_time,
              arrival_time_target,
              n_boardings) VALUES (%s) ''' % (", ".join(["?" for x in range(6)]))
        #self.conn.executemany(insert_journeys_stmt, journey_list)

        self._execute_function(insert_journeys_stmt, journey_list)
        self.conn.commit()

    @timeit
    def _execute_function(self, statement, rows):
        self.conn.execute('BEGIN EXCLUSIVE')
        last_id = self._get_largest_journey_id()
        rows = [[x[0]+last_id] + x[1:] for x in rows]
        self.conn.executemany(statement, rows)

    def _insert_journeys_with_route_into_db(self, stop_I_to_journey_labels, target_stop):
        print("Collecting journey and connection data")
        journey_id = (self._get_largest_journey_id() if self._get_largest_journey_id() else 0) + 1
        journey_list = []
        connection_list = []
        label = None
        for i, (origin_stop, labels) in enumerate(stop_I_to_journey_labels.items(), start=1):
            # tot = len(stop_profiles)
            #print("\r Stop " + str(i) + " of " + str(tot), end='', flush=True)

            assert (isinstance(stop_I_to_journey_labels[origin_stop], list))

            for label in labels:
                assert (isinstance(label, LabelTimeAndRoute) or isinstance(label, LabelTimeBoardingsAndRoute))
                # We need to "unpack" the journey to actually figure out where the trip went
                # (there can be several targets).
                if label.departure_time == label.arrival_time_target:
                    print("Weird label:", label)
                    continue

                target_stop, new_connection_values, route_stops = self._collect_connection_data(journey_id, label)
                if origin_stop == target_stop:
                    continue

                if isinstance(label, LabelTimeBoardingsAndRoute):
                    values = [int(journey_id),
                              int(origin_stop),
                              int(target_stop),
                              int(label.departure_time),
                              int(label.arrival_time_target),
                              label.n_boardings,
                              label.movement_duration,
                              route_stops]
                else:
                    values = [int(journey_id),
                              int(origin_stop),
                              int(target_stop),
                              int(label.departure_time),
                              int(label.arrival_time_target),
                              label.movement_duration,
                              route_stops]

                journey_list.append(values)
                connection_list += new_connection_values
                journey_id += 1

        print("Inserting journeys into database")
        if label:
            if isinstance(label, LabelTimeBoardingsAndRoute):
                insert_journeys_stmt = '''INSERT INTO journeys(
                      journey_id,
                      from_stop_I,
                      to_stop_I,
                      departure_time,
                      arrival_time_target,
                      n_boardings,
                      movement_duration,
                      route) VALUES (%s) ''' % (", ".join(["?" for x in range(8)]))
            else:
                insert_journeys_stmt = '''INSERT INTO journeys(
                      journey_id,
                      from_stop_I,
                      to_stop_I,
                      departure_time,
                      arrival_time_target,
                      movement_duration,
                      route) VALUES (%s) ''' % (", ".join(["?" for x in range(7)]))
            self.conn.executemany(insert_journeys_stmt, journey_list)

            print("Inserting legs into database")
            insert_legs_stmt = '''INSERT INTO legs(
                                  journey_id,
                                  from_stop_I,
                                  to_stop_I,
                                  departure_time,
                                  arrival_time_target,
                                  trip_I,
                                  seq,
                                  leg_stops) VALUES (%s) ''' % (", ".join(["?" for x in range(8)]))
            self.conn.executemany(insert_legs_stmt, connection_list)
            self.routing_parameters["target_list"] += (str(target_stop) + ",")
            self.conn.commit()


    def create_index_for_journeys_table(self):
        self.conn.execute("PRAGMA temp_store=2")
        self.conn.commit()
        self.conn.execute("CREATE INDEX IF NOT EXISTS journeys_to_stop_I_idx ON journeys (to_stop_I)")

    def _collect_connection_data(self, journey_id, label):
        target_stop = None
        cur_label = label
        seq = 1
        value_list = []
        route_stops = []
        leg_stops = []
        prev_trip_id = None
        connection = None
        leg_departure_time = None
        leg_departure_stop = None
        leg_arrival_time = None
        leg_arrival_stop = None
        while True:
            if isinstance(cur_label.connection, Connection):
                connection = cur_label.connection
                if connection.trip_id:
                    trip_id = connection.trip_id
                else:
                    trip_id = -1

                # In case of new leg
                if prev_trip_id != trip_id:
                    route_stops.append(connection.departure_stop)
                    if prev_trip_id:
                        leg_stops.append(connection.departure_stop)

                        values = (
                            int(journey_id),
                            int(leg_departure_stop),
                            int(leg_arrival_stop),
                            int(leg_departure_time),
                            int(leg_arrival_time),
                            int(prev_trip_id),
                            int(seq),
                            ','.join([str(x) for x in leg_stops])
                                )
                        value_list.append(values)
                        seq += 1
                        leg_stops = []

                    leg_departure_stop = connection.departure_stop
                    leg_departure_time = connection.departure_time
                leg_arrival_time = connection.arrival_time
                leg_arrival_stop = connection.arrival_stop
                leg_stops.append(connection.departure_stop)
                target_stop = connection.arrival_stop
                prev_trip_id = trip_id

            if not cur_label.previous_label:
                leg_stops.append(connection.arrival_stop)
                values = (
                    int(journey_id),
                    int(leg_departure_stop),
                    int(leg_arrival_stop),
                    int(leg_departure_time),
                    int(leg_arrival_time),
                    int(prev_trip_id),
                    int(seq),
                    ','.join([str(x) for x in leg_stops])
                )
                value_list.append(values)
                break

            cur_label = cur_label.previous_label
        route_stops.append(target_stop)
        route_stops = ','.join([str(x) for x in route_stops])
        return target_stop, value_list, route_stops

    def populate_additional_journey_columns(self):
        # self.add_fastest_path_column()
        # self.add_time_to_prev_journey_fp_column()
        self.compute_journey_time_components()
        self.calculate_pre_journey_waiting_times_ignoring_direct_walk()

    def get_od_pairs(self):
        cur = self.conn.cursor()
        if not self.od_pairs:
            cur.execute('SELECT from_stop_I, to_stop_I FROM journeys GROUP BY from_stop_I, to_stop_I')
            self.od_pairs = cur.fetchall()
        return self.od_pairs

    def get_targets(self):
        cur = self.conn.cursor()
        if not self._targets:
            cur.execute('SELECT to_stop_I FROM journeys GROUP BY to_stop_I')
            self._targets = [target[0] for target in cur.fetchall()]
        return self._targets

    def get_origins(self):
        cur = self.conn.cursor()
        if not self._origins:
            cur.execute('SELECT from_stop_I FROM journeys GROUP BY from_stop_I')
            self._origins = [origin[0] for origin in cur.fetchall()]
        return self._origins

    def get_table_with_coordinates(self, table_name, target=None):
        df = self.get_table_as_dataframe(table_name, target)
        return self.gtfs.add_coordinates_to_df(df, join_column='from_stop_I')

    def get_table_as_dataframe(self, table_name, to_stop_I_target=None):
        query = "SELECT * FROM " + table_name
        if to_stop_I_target:
            query += " WHERE to_stop_I = %s" % to_stop_I_target
        return pd.read_sql_query(query, self.conn)

    @timeit
    def add_fastest_path_column(self):
        print("adding fastest path column")
        cur = self.conn.cursor()
        for target in self.get_targets():
            fastest_path_journey_ids = []
            for origin in self.get_origins():
                cur.execute('SELECT departure_time, arrival_time_target, journey_id FROM journeys '
                            'WHERE from_stop_I = ? AND to_stop_I = ? '
                            'ORDER BY departure_time ASC', (origin, target))
                all_trips = cur.fetchall()
                all_labels = [LabelTimeAndRoute(x[0], x[1], x[2], False) for x in all_trips] #putting journey_id as movement_duration
                all_fp_labels = compute_pareto_front(all_labels, finalization=False, ignore_n_boardings=True)
                fastest_path_journey_ids.append(all_fp_labels)

            fastest_path_journey_ids = [(1, x.movement_duration) for sublist in fastest_path_journey_ids for x in sublist]
            cur.executemany("UPDATE journeys SET fastest_path = ? WHERE journey_id = ?", fastest_path_journey_ids)
        self.conn.commit()

    @timeit
    def add_time_to_prev_journey_fp_column(self):
        print("adding pre journey waiting time")
        cur = self.conn.cursor()
        for target in self.get_targets():

            cur.execute('SELECT journey_id, from_stop_I, to_stop_I, departure_time FROM journeys '
                        'WHERE fastest_path = 1 AND to_stop_I = ? '
                        'ORDER BY from_stop_I, to_stop_I, departure_time ', (target[0],))

            all_trips = cur.fetchall()
            time_to_prev_journey = []
            prev_departure_time = None
            prev_origin = None
            prev_destination = None
            for trip in all_trips:
                journey_id = trip[0]
                from_stop_I = trip[1]
                to_stop_I = trip[2]
                departure_time = trip[3]
                if prev_origin != from_stop_I or prev_destination != to_stop_I:
                    prev_departure_time = None
                if prev_departure_time:
                    time_to_prev_journey.append((departure_time - prev_departure_time, journey_id))
                prev_origin = from_stop_I
                prev_destination = to_stop_I
                prev_departure_time = departure_time
            cur.executemany("UPDATE journeys SET pre_journey_wait_fp = ? WHERE journey_id = ?", time_to_prev_journey)
        self.conn.commit()

    @timeit
    def compute_journey_time_components(self):
        print("adding journey components")
        cur = self.conn.cursor()
        cur.execute("UPDATE journeys SET journey_duration = arrival_time_target - departure_time")

        if self.track_route:
            cur.execute("UPDATE journeys "
                        "SET "
                        "in_vehicle_duration = "
                        "(SELECT sum(arrival_time_target - departure_time) AS in_vehicle_duration FROM legs "
                        "WHERE journeys.journey_id = legs.journey_id AND trip_I != -1 GROUP BY journey_id)")
            cur.execute("UPDATE journeys "
                        "SET "
                        "walking_duration = "
                        "(SELECT sum(arrival_time_target - departure_time) AS walking_duration FROM legs "
                        "WHERE journeys.journey_id = legs.journey_id AND trip_I < 0 GROUP BY journey_id)")
            cur.execute("UPDATE journeys "
                        "SET transfer_wait_duration = journey_duration - in_vehicle_duration - walking_duration")
        self.conn.commit()

    def _journey_label_generator(self, destination_stop_Is=None, origin_stop_Is=None):
        """
        Parameters
        ----------
        destination_stop_Is: list-like
        origin_stop_Is: list-like

        Yields
        ------
        (origin_stop_I, destination_stop_I, journey_labels) : tuple
        """
        conn = self.conn
        conn.row_factory = sqlite3.Row
        if destination_stop_Is is None:
            destination_stop_Is = self.get_targets()
        if origin_stop_Is is None:
            origin_stop_Is = self.get_origins()

        for destination_stop_I in destination_stop_Is:
            if self.track_route:
                label_features = "journey_id, from_stop_I, to_stop_I, n_boardings, movement_duration, " \
                                 "journey_duration, in_vehicle_duration, transfer_wait_duration, walking_duration, " \
                                 "departure_time, arrival_time_target"""
            else:
                label_features = "journey_id, from_stop_I, to_stop_I, n_boardings, departure_time, " \
                                 "arrival_time_target"
            sql = "SELECT " + label_features + " FROM journeys WHERE to_stop_I = %s" % destination_stop_I

            df = pd.read_sql_query(sql, self.conn)
            for origin_stop_I in origin_stop_Is:
                selection = df.loc[df['from_stop_I'] == origin_stop_I]
                journey_labels = []
                for journey in selection.to_dict(orient='records'):
                    journey["pre_journey_wait_fp"] = -1
                    try:
                        journey_labels.append(LabelGeneric(journey))
                    except:
                        print(journey)
                yield origin_stop_I, destination_stop_I, journey_labels

    def get_node_profile_time_analyzer(self, target, origin, start_time_dep, end_time_dep):
        sql = """SELECT journey_id, from_stop_I, to_stop_I, n_boardings, movement_duration, journey_duration,
        in_vehicle_duration, transfer_wait_duration, walking_duration, departure_time, arrival_time_target
        FROM journeys WHERE to_stop_I = %s AND from_stop_I = %s""" % (target, origin)
        df = pd.read_sql_query(sql, self.conn)
        journey_labels = []
        for journey in df.to_dict(orient='records'):
            journey_labels.append(LabelGeneric(journey))

        fpa = FastestPathAnalyzer(journey_labels,
                                  start_time_dep,
                                  end_time_dep,
                                  walk_duration=float('inf'),  # walking time
                                  label_props_to_consider=list(self.journey_properties.keys()))
        return fpa.get_time_analyzer()

    def _get_node_profile_analyzer_time_and_veh_legs(self, target, origin, start_time_dep, end_time_dep):
        sql = """SELECT from_stop_I, to_stop_I, n_boardings, departure_time, arrival_time_target FROM journeys WHERE to_stop_I = %s AND from_stop_I = %s""" % (target, origin)
        df = pd.read_sql_query(sql, self.conn)

        journey_labels = []
        for journey in df.itertuples():
            departure_time = journey.departure_time
            arrival_time_target = journey.arrival_time_target
            n_boardings = journey.n_boardings
            journey_labels.append(LabelTimeWithBoardingsCount(departure_time,
                                                              arrival_time_target,
                                                              n_boardings,
                                                              first_leg_is_walk=float('nan')))

        # This ought to be optimized...
        query = """SELECT d, d_walk FROM stop_distances WHERE to_stop_I = %s AND from_stop_I = %s""" % (target, origin)
        df = self.gtfs.execute_custom_query_pandas(query)
        if len(df) > 0:
            walk_duration = float(df['d_walk']) / self.routing_params_input['walk_speed']
        else:
            walk_duration = float('inf')
        analyzer = NodeProfileAnalyzerTimeAndVehLegs(journey_labels,
                                                     walk_duration,  # walking time
                                                     start_time_dep,
                                                     end_time_dep)
        return analyzer

    def read_travel_impedance_measure_from_table(self,
                                                 travel_impedance_measure,
                                                 from_stop_I=None,
                                                 to_stop_I=None,
                                                 statistic=None):
        """
        Recover pre-computed travel_impedance between od-pairs from the database.

        Returns
        -------
        values: number | Pandas DataFrame
        """
        to_select = []
        where_clauses = []
        to_select.append("from_stop_I")
        to_select.append("to_stop_I")
        if from_stop_I is not None:
            where_clauses.append("from_stop_I=" + str(int(from_stop_I)))
        if to_stop_I is not None:
            where_clauses.append("to_stop_I=" + str(int(to_stop_I)))
        where_clause = ""
        if len(where_clauses) > 0:
            where_clause = " WHERE " + " AND ".join(where_clauses)
        if not statistic:
            to_select.extend(["min", "mean", "median", "max"])
        else:
            to_select.append(statistic)
        to_select_clause = ",".join(to_select)
        if not to_select_clause:
            to_select_clause = "*"
        sql = "SELECT " + to_select_clause + " FROM " + travel_impedance_measure + where_clause + ";"
        df = pd.read_sql(sql, self.conn)
        return df

    def __get_travel_impedance_measure_dict(self,
                                            origin,
                                            target,
                                            journey_labels,
                                            analysis_start_time,
                                            analysis_end_time):
        measure_summaries = {}
        kwargs = {"from_stop_I": origin, "to_stop_I": target}
        walking_distance = self.gtfs.get_stop_distance(origin, target)

        if walking_distance:
            walking_duration = walking_distance / self.routing_params_input["walk_speed"]
        else:
            walking_duration = float("inf")
        fpa = FastestPathAnalyzer(journey_labels,
                                  analysis_start_time,
                                  analysis_end_time,
                                  walk_duration=walking_duration,  # walking time
                                  label_props_to_consider=list(self.journey_properties.keys()),
                                  **kwargs)
        temporal_distance_analyzer = fpa.get_temporal_distance_analyzer()
        # Note: the summary_as_dict automatically includes also the from_stop_I and to_stop_I -fields.
        measure_summaries["temporal_distance"] = temporal_distance_analyzer.summary_as_dict()
        fpa.calculate_pre_journey_waiting_times_ignoring_direct_walk()
        for key, (value_no_next_journey, value_cutoff) in self.journey_properties.items():
            value_cutoff = walking_duration if value_cutoff == _T_WALK_STR else value_cutoff
            value_no_next_journey = walking_duration if value_no_next_journey == _T_WALK_STR else value_no_next_journey
            if key == "pre_journey_wait_fp":
                property_analyzer = fpa.get_prop_analyzer_for_pre_journey_wait()
            else:
                property_analyzer = fpa.get_prop_analyzer_flat(key, value_no_next_journey, value_cutoff)
            measure_summaries[key] = property_analyzer.summary_as_dict()
        return measure_summaries

    def compute_travel_impedance_measures_for_target(self,
                                                     analysis_start_time,
                                                     analysis_end_time,
                                                     target, origins=None):
        if origins is None:
            origins = self.get_origins()
        measure_to_measure_summary_dicts = {}
        for measure in ["temporal_distance"] + list(self.journey_properties):
            measure_to_measure_summary_dicts[measure] = []
        for origin, target, journey_labels in self._journey_label_generator([target], origins):
            measure_summary_dicts_for_pair = \
            self.__get_travel_impedance_measure_dict(
                origin, target, journey_labels,
                analysis_start_time, analysis_end_time
            )
            for measure in measure_summary_dicts_for_pair:
                measure_to_measure_summary_dicts[measure].append(measure_summary_dicts_for_pair[measure])
        return measure_to_measure_summary_dicts



    @timeit
    def compute_and_store_travel_impedance_measures(self,
                                                    analysis_start_time,
                                                    analysis_end_time,
                                                    targets=None,
                                                    origins=None):
        measure_to_measure_summary_dicts = {}
        for travel_impedance_measure in self.travel_impedance_measure_names:
            self._create_travel_impedance_measure_table(travel_impedance_measure)

        print("Computing total number of origins and targets..", end='', flush=True)
        if targets is None:
            targets = self.get_targets()
        if origins is None:
            origins = self.get_origins()
        print("\rComputed total number of origins and targets")
        n_pairs_tot = len(origins) * len(targets)

        def _flush_data_to_db(results):
            for travel_impedance_measure, data in results.items():
                self._insert_travel_impedance_data_to_db(travel_impedance_measure, data)
            for travel_impedance_measure in self.travel_impedance_measure_names:
                results[travel_impedance_measure] = []

        # This initializes the meaasure_to_measure_summary_dict properly
        _flush_data_to_db(measure_to_measure_summary_dicts)

        for i, (origin, target, journey_labels) in enumerate(self._journey_label_generator(targets, origins)):
            measure_summary_dicts_for_pair = self.__get_travel_impedance_measure_dict(origin, target, journey_labels,
                                                                      analysis_start_time, analysis_end_time)
            for measure in measure_summary_dicts_for_pair:
                measure_to_measure_summary_dicts[measure].append(measure_summary_dicts_for_pair[measure])

            if i % 1000 == 0: # update in batches of 1000
                print("\r", i, "/", n_pairs_tot, " : ", "%.2f" % round(float(i) / n_pairs_tot, 3), end='', flush=True)
                _flush_data_to_db(measure_to_measure_summary_dicts)

        # flush everything that remains
        _flush_data_to_db(measure_to_measure_summary_dicts)

    def create_indices_for_travel_impedance_measure_tables(self):
        for travel_impedance_measure in self.travel_impedance_measure_names:
            self._create_index_for_travel_impedance_measure_table(travel_impedance_measure)

    @timeit
    def calculate_pre_journey_waiting_times_ignoring_direct_walk(self):
        all_fp_labels = []
        for origin, target, journey_labels in self._journey_label_generator():
            if not journey_labels:
                continue
            fpa = FastestPathAnalyzer(journey_labels,
                                      self.routing_parameters["routing_start_time_dep"],
                                      self.routing_parameters["routing_end_time_dep"],
                                      walk_duration=float('inf'))
            fpa.calculate_pre_journey_waiting_times_ignoring_direct_walk()
            all_fp_labels += fpa.get_fastest_path_labels()
        self.update_journey_from_labels(all_fp_labels, "pre_journey_wait_fp")

    def update_journey_from_labels(self, labels, attribute):
        cur = self.conn.cursor()
        insert_tuples = []
        for label in labels:
            insert_tuples.append((getattr(label, attribute), getattr(label, "journey_id")))

        sql = "UPDATE journeys SET %s = ? WHERE journey_id = ?" % (attribute,)
        cur.executemany(sql, insert_tuples)
        self.conn.commit()

    def _create_travel_impedance_measure_table(self, travel_impedance_measure):
        print("creating table: ", travel_impedance_measure)
        self.conn.execute("CREATE TABLE IF NOT EXISTS " + travel_impedance_measure + " (from_stop_I INT, "
                                                                  "to_stop_I INT, "
                                                                  "min REAL, "
                                                                  "max REAL, "
                                                                  "median REAL, "
                                                                  "mean REAL, "
                                                                  "UNIQUE (from_stop_I, to_stop_I))")

    def _insert_travel_impedance_data_to_db(self, travel_impedance_measure_name, data):
        """
        Parameters
        ----------
        travel_impedance_measure_name: str
        data: list[dict]
            Each list element must contain keys:
            "from_stop_I", "to_stop_I", "min", "max", "median" and "mean"
        """
        f = float
        data_tuple = [(x["from_stop_I"], x["to_stop_I"], f(x["min"]), f(x["max"]), f(x["median"]), f(x["mean"])) for x in data]
        insert_stmt = '''INSERT OR REPLACE INTO ''' + travel_impedance_measure_name + ''' (
                              from_stop_I,
                              to_stop_I,
                              min,
                              max,
                              median,
                              mean) VALUES (?, ?, ?, ?, ?, ?) '''
        self.conn.executemany(insert_stmt, data_tuple)
        self.conn.commit()

    def create_index_for_journeys_table(self):
        self.conn.execute("CREATE INDEX IF NOT EXISTS journeys_to_stop_I_idx ON journeys (to_stop_I)")

    def _create_index_for_travel_impedance_measure_table(self, travel_impedance_measure_name):
        table = travel_impedance_measure_name
        sql_from = "CREATE INDEX IF NOT EXISTS " + table + "_from_stop_I ON " + table + " (from_stop_I)"
        sql_to = "CREATE INDEX IF NOT EXISTS " + table + "_to_stop_I ON " + table + " (to_stop_I)"
        self.conn.execute(sql_from)
        self.conn.execute(sql_to)
        self.conn.commit()


    @timeit
    def initialize_comparison_tables(self, diff_db_path, before_db_tuple, after_db_tuple):
        self.diff_conn = sqlite3.connect(diff_db_path)

        self.diff_conn = attach_database(self.diff_conn, before_db_tuple[0], name=before_db_tuple[1])
        self.diff_conn = attach_database(self.diff_conn, after_db_tuple[0], name=after_db_tuple[1])

        for table in self.travel_impedance_measure_names:
            self.diff_conn.execute("CREATE TABLE IF NOT EXISTS diff_" + table +
                                   " (from_stop_I, to_stop_I, diff_min, diff_max, diff_median, diff_mean)")
            insert_stmt = "INSERT OR REPLACE INTO diff_" + table + \
                          "(from_stop_I, to_stop_I, diff_min, diff_max, diff_median, diff_mean) " \
                          "SELECT t1.from_stop_I, t1.to_stop_I, " \
                          "t1.min - t2.min AS diff_min, " \
                          "t1.max - t2.max AS diff_max, " \
                          "t1.median - t2.median AS diff_median, " \
                          "t1.mean - t2.mean AS diff_mean " \
                          "FROM " + before_db_tuple[1] + "." + table + " AS t1, " \
                          + before_db_tuple[1] + "." + table + " AS t2 " \
                                                               "WHERE t1.from_stop_I = t2.from_stop_I " \
                                                               "AND t1.to_stop_I = t2.to_stop_I "
            self.diff_conn.execute(insert_stmt)
            self.diff_conn.commit()

    def initialize_database(self):
        self._set_up_database()
        self._initialize_parameter_table()
        print("Database initialized!")

    def _set_up_database(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS parameters(
                             key TEXT UNIQUE,
                             value BLOB)''')
        if self.track_route:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS journeys(
                             journey_id INTEGER PRIMARY KEY,
                             from_stop_I INT,
                             to_stop_I INT,
                             departure_time INT,
                             arrival_time_target INT,
                             n_boardings INT,
                             movement_duration INT,
                             route TEXT,
                             journey_duration INT,
                             pre_journey_wait_fp INT,
                             in_vehicle_duration INT,
                             transfer_wait_duration INT,
                             walking_duration INT,
                             fastest_path INT)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS legs(
                         journey_id INT,
                         from_stop_I INT,
                         to_stop_I INT,
                         departure_time INT,
                         arrival_time_target INT,
                         trip_I INT,
                         seq INT,
                         leg_stops TEXT)''')
            """
            self.conn.execute('''CREATE TABLE IF NOT EXISTS nodes(
                         stop_I INT,
                         agg_temp_distances REAL,
                         agg_journey_duration REAL,
                         agg_boardings REAL,
                         agg_transfer_wait REAL,
                         agg_pre_journey_wait REAL,
                         agg_walking_duration REAL)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS od_pairs(
                         from_stop_I INT,
                         to_stop_I INT,
                         avg_temp_distance REAL,
                         agg_journey_duration REAL,
                         agg_boardings REAL,
                         agg_transfer_wait REAL,
                         agg_pre_journey_wait REAL,
                         agg_walking_duration REAL)''')


            self.conn.execute('''CREATE TABLE IF NOT EXISTS sections(
                         from_stop_I INT,
                         to_stop_I INT,
                         from_stop_pair_I INT,
                         to_stop_pair_I INT,
                         avg_temp_distance INT,
                         avg_journey_duration INT,
                         n_trips INT)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS transfer_nodes(
                         from_stop_I INT,
                         to_stop_I INT,
                         from_stop_pair_I INT,
                         to_stop_pair_I INT,
                         avg_waiting_time INT,
                         n_trips INT)''')
            """
        else:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS journeys(
                         journey_id INTEGER PRIMARY KEY,
                         from_stop_I INT,
                         to_stop_I INT,
                         departure_time INT,
                         arrival_time_target INT,
                         n_boardings INT,
                         journey_duration INT,
                         time_to_prev_journey_fp INT,
                         fastest_path INT)''')

        self.conn.commit()

    def _initialize_parameter_table(self):

        parameters = Parameters(self.conn)

        parameters["multiple_targets"] = self.multitarget_routing
        parameters["gtfs_dir"] = self.gtfs_path
        for param in ["location_name",
                      "lat_median",
                      "lon_median",
                      "start_time_ut",
                      "end_time_ut",
                      "start_date",
                      "end_date"]:
            parameters[param] = self.gtfs_meta[param]
        parameters["target_list"] = ","
        for key, value in self.routing_params_input.items():
            parameters[key] = value
        self.conn.commit()

    def create_indices(self):
        # Next 3 lines are python 3.6 work-arounds again.
        self.conn.isolation_level = None  # former default of autocommit mode
        cur = self.conn.cursor()
        cur.execute('VACUUM;')
        self.conn.isolation_level = ''  # back to python default
        # end python3.6 workaround
        print("Analyzing...")
        cur.execute('ANALYZE')
        print("Indexing")
        cur = self.conn.cursor()
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_route ON journeys (route)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_jid ON journeys (journey_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_fid ON journeys (from_stop_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_tid ON journeys (to_stop_I)')

        if self.track_route:
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_jid ON legs (journey_id)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_trid ON legs (trip_I)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_fid ON legs (from_stop_I)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_tid ON legs (to_stop_I)')
        self.conn.commit()


class DiffDataManager:
    def __init__(self, diff_db_path):
        self.conn = sqlite3.connect(diff_db_path)

    def initialize_journey_comparison_tables(self, tables, before_db_tuple, after_db_tuple):
        before_db_path = before_db_tuple[0]
        before_db_name = before_db_tuple[1]
        after_db_path = after_db_tuple[0]
        after_db_name = after_db_tuple[1]

        self.conn = self.attach_database(before_db_path, name=before_db_name)
        self.conn = self.attach_database(after_db_path, name=after_db_name)

        for table in tables:
            self.conn.execute("CREATE TABLE IF NOT EXISTS diff_" + table +
                              "(from_stop_I INT, to_stop_I INT, "
                              "diff_min INT, diff_max INT, diff_median INT, diff_mean INT, "
                              "rel_diff_min REAL, rel_diff_max REAL, rel_diff_median REAL, rel_diff_mean REAL)")
            insert_stmt = "INSERT OR REPLACE INTO diff_" + table + \
                          " (from_stop_I, to_stop_I, diff_min, diff_max, diff_median, diff_mean, " \
                          "rel_diff_min, rel_diff_max, rel_diff_median, rel_diff_mean) " \
                          "SELECT " \
                          "t1.from_stop_I, " \
                          "t1.to_stop_I, " \
                          "t1.min - t2.min AS diff_min, " \
                          "t1.max - t2.max AS diff_max, " \
                          "t1.median - t2.median AS diff_median, " \
                          "t1.mean - t2.mean AS diff_mean, " \
                          "(t1.min - t2.min)*1.0/t2.min AS rel_diff_min, " \
                          "(t1.max - t2.max)*1.0/t2.max AS rel_diff_max, " \
                          "(t1.median - t2.median)*1.0/t2.median AS rel_diff_median, " \
                          "(t1.mean - t2.mean)*1.0/t2.mean AS rel_diff_mean " \
                          "FROM " + after_db_name + "." + table + " AS t1, "\
                          + before_db_name + "." + table + \
                          " AS t2 WHERE t1.from_stop_I = t2.from_stop_I AND t1.to_stop_I = t2.to_stop_I "
            self.conn.execute(insert_stmt)
            self.conn.commit()

    def attach_database(self, other_db_path, name="other"):
        cur = self.conn.cursor()
        cur.execute("ATTACH '%s' AS '%s'" % (str(other_db_path), name))
        cur.execute("PRAGMA database_list")
        print("other database attached:", cur.fetchall())
        return self.conn

    def get_table_with_coordinates(self, gtfs, table_name, target=None, use_relative=False):
        df = self.get_table_as_dataframe(table_name, use_relative, target)
        return gtfs.add_coordinates_to_df(df, join_column='from_stop_I')

    def get_table_as_dataframe(self, table_name, use_relative, target=None):
        if use_relative:
            query = "SELECT from_stop_I, to_stop_I, rel_diff_min, rel_diff_max, rel_diff_median, rel_diff_mean FROM "\
                    + table_name
        else:
            query = "SELECT from_stop_I, to_stop_I, diff_min, diff_max, diff_median, diff_mean FROM " + table_name
        if target:
            query += " WHERE to_stop_I = %s" % target
        return pd.read_sql_query(query, self.conn)

    def get_temporal_distance_change_o_d_pairs(self, target, threshold):
        cur = self.conn.cursor()
        query = """SELECT from_stop_I FROM diff_temporal_distance
                    WHERE to_stop_I = %s AND abs(diff_mean) >= %s""" % (target, threshold)
        rows = [x[0] for x in cur.execute(query).fetchall()]
        return rows

    def get_largest_component(self, target, threshold=180):
        query = """SELECT diff_pre_journey_wait_fp.from_stop_I AS stop_I, 
                    diff_pre_journey_wait_fp.diff_mean AS pre_journey_wait, 
                    diff_in_vehicle_duration.diff_mean AS in_vehicle_duration,
                    diff_transfer_wait_duration.diff_mean AS transfer_wait, 
                    diff_walking_duration.diff_mean AS walking_duration,
                    diff_temporal_distance.diff_mean AS temporal_distance
                    FROM diff_pre_journey_wait_fp, diff_in_vehicle_duration, 
                    diff_transfer_wait_duration, diff_walking_duration, diff_temporal_distance
                    WHERE diff_pre_journey_wait_fp.rowid = diff_in_vehicle_duration.rowid
                    AND diff_pre_journey_wait_fp.rowid = diff_transfer_wait_duration.rowid
                    AND diff_pre_journey_wait_fp.rowid = diff_walking_duration.rowid
                    AND diff_pre_journey_wait_fp.rowid = diff_temporal_distance.rowid
                    AND diff_pre_journey_wait_fp.to_stop_I = %s""" % (target,)
        df = pd.read_sql_query(query, self.conn)
        df['max_component'] = df[["pre_journey_wait", "in_vehicle_duration", "transfer_wait", "walking_duration"]].idxmax(axis=1)
        df['max_value'] = df[["pre_journey_wait", "in_vehicle_duration", "transfer_wait", "walking_duration"]].max(axis=1)

        mask = (df['max_value'] < threshold)

        df.loc[mask, 'max_component'] = "no_change_within_threshold"

        df['min_component'] = df[["pre_journey_wait", "in_vehicle_duration", "transfer_wait", "walking_duration"]].idxmin(axis=1)
        df['min_value'] = df[["pre_journey_wait", "in_vehicle_duration", "transfer_wait", "walking_duration"]].min(axis=1)

        mask = (df['min_value'] > -1 * threshold)

        df.loc[mask, 'min_component'] = "no_change_within_threshold"

        return df


class Parameters(object):
    """
    This provides dictionary protocol for updating parameters table, similar to GTFS metadata ("meta table").
    """

    def __init__(self, conn):
        self._conn = conn
        self._conn.execute("CREATE TABLE IF NOT EXISTS parameters (key, value)")

    def __setitem__(self, key, value):
        self._conn.execute("INSERT OR REPLACE INTO parameters('key', 'value') VALUES (?, ?)", (key, value))
        self._conn.commit()

    def __getitem__(self, key):
        cur = self._conn.cursor()
        cur.execute("SELECT value FROM parameters WHERE key=?", (key,))
        val = cur.fetchone()
        if not val:
            raise KeyError("This journey db does not have parameter: %s" % key)
        return val[0]

    def __delitem__(self, key):
        self._conn.execute("DELETE FROM parameters WHERE key=?", (key,))
        self._conn.commit()

    def __iter__(self):
        cur = self._conn.execute('SELECT key FROM parameters ORDER BY key')
        return (x[0] for x in cur)

    def __contains__(self, key):
        val = self._conn.execute('SELECT value FROM parameters WHERE key=?',
                                 (key,)).fetchone()
        return val is not None

    def get(self, key, default=None):
        val = self._conn.execute('SELECT value FROM parameters WHERE key=?',
                                 (key,)).fetchone()
        if not val:
            return default
        return val[0]

    def items(self):
        cur = self._conn.execute('SELECT key, value FROM parameters ORDER BY key')
        return cur

    def keys(self):
        cur = self._conn.execute('SELECT key FROM parameters ORDER BY key')
        return cur

    def values(self):
        cur = self._conn.execute('SELECT value FROM parameters ORDER BY key')
        return cur
