import os
import sqlite3

from gtfspy.routing.connection import Connection
from gtfspy.gtfs import GTFS
from gtfspy.routing.label import LabelTimeAndRoute, LabelTimeWithBoardingsCount, LabelTimeBoardingsAndRoute, \
    compute_pareto_front
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.util import timeit


class JourneyDataManager:
    def __init__(self, gtfs_dir, routing_params, journey_db_dir=None, multitarget_routing=False, close_connection=True,
                 track_route=False, track_vehicle_legs=True):
        """

        :param gtfs: GTFS object
        :param list_of_stop_profiles: dict of NodeProfileMultiObjective
        :param multitarget_routing: bool
        """
        self.close_connection = close_connection
        self.routing_params = routing_params
        self.multitarget_routing = multitarget_routing
        self.track_route = track_route
        self.track_vehicle_legs = track_vehicle_legs
        self.gtfs_dir = gtfs_dir
        self.gtfs = GTFS(self.gtfs_dir)
        self.gtfs_meta = self.gtfs.meta
        self.gtfs._dont_close = True
        print('location_name: ', self.gtfs_meta["location_name"])
        self.conn = None
        self.od_pairs = None
        self.targets = None
        self.origins = None
        if journey_db_dir:
            if os.path.isfile(journey_db_dir):
                self.conn = sqlite3.connect(journey_db_dir)
                self.parameters = Parameters(self.conn)
                self._check_that_dbs_match()

            else:
                raise Exception("Database specified does not exist, use run_preparations() method first")

    def __del__(self):
        self.gtfs._dont_close = False
        if self.conn:
            self.conn.close()

    @timeit
    def import_journey_data_single_stop(self, stop_profiles, target_stop):
        cur = self.conn.cursor()
        self.conn.isolation_level = 'EXCLUSIVE'
        cur.execute('PRAGMA synchronous = OFF;')
        if not self.target_in_db(target_stop):
            if self.track_route:
                self._insert_journeys_with_route_into_db(stop_profiles, target_stop=target_stop)
            else:
                self._insert_journeys_into_db_no_route(stop_profiles, target_stop=target_stop)

            if self.close_connection:
                self.conn.close()

        print("Finished import process")

    def target_in_db(self, target_stop):
        return "," + str(target_stop) + "," in self.parameters["target_list"]

    def _check_that_dbs_match(self):
        for key, value in self.parameters.items():
            if key in self.gtfs_meta.keys():
                assert self.gtfs_meta[key] == value

    def _check_last_journey_id(self):
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

                values = [journey_id,
                          origin_stop,
                          target_stop,
                          int(label.departure_time),
                          int(label.arrival_time_target),
                          label.n_boardings]

                journey_list.append(values)
                journey_id += 1
        print("Inserting journeys into database")
        insert_journeys_stmt = '''INSERT INTO journeys(
              journey_id,
              from_stop_I,
              to_stop_I,
              dep_time,
              arr_time,
              n_boardings) VALUES (%s) ''' % (", ".join(["?" for x in range(6)]))
        #self.conn.executemany(insert_journeys_stmt, journey_list)

        self._execute_function(insert_journeys_stmt, journey_list)
        self.conn.commit()

    @timeit
    def _execute_function(self, statement, rows):
        self.conn.execute('BEGIN EXCLUSIVE')
        last_id = self._check_last_journey_id()
        rows = [[x[0]+last_id] + x[1:] for x in rows]
        self.conn.executemany(statement, rows)

    def _insert_journeys_with_route_into_db(self, stop_profiles, target_stop):
        print("Collecting journey and connection data")
        journey_id = (self._check_last_journey_id() if self._check_last_journey_id() else 0) + 1
        journey_list = []
        connection_list = []
        label = None
        tot = len(stop_profiles)
        for i, (origin_stop, labels) in enumerate(stop_profiles.items(), start=1):
            #print("\r Stop " + str(i) + " of " + str(tot), end='', flush=True)

            assert (isinstance(stop_profiles[origin_stop], list))

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

        print()
        print("Inserting journeys into database")
        if label:
            if isinstance(label, LabelTimeBoardingsAndRoute):
                insert_journeys_stmt = '''INSERT INTO journeys(
                      journey_id,
                      from_stop_I,
                      to_stop_I,
                      dep_time,
                      arr_time,
                      n_boardings,
                      movement_duration,
                      route) VALUES (%s) ''' % (", ".join(["?" for x in range(8)]))
            else:
                insert_journeys_stmt = '''INSERT INTO journeys(
                      journey_id,
                      from_stop_I,
                      to_stop_I,
                      dep_time,
                      arr_time,
                      movement_duration,
                      route) VALUES (%s) ''' % (", ".join(["?" for x in range(7)]))
            self.conn.executemany(insert_journeys_stmt, journey_list)

            print("Inserting legs into database")
            insert_legs_stmt = '''INSERT INTO legs(
                                  journey_id,
                                  from_stop_I,
                                  to_stop_I,
                                  dep_time,
                                  arr_time,
                                  trip_I,
                                  seq,
                                  leg_stops) VALUES (%s) ''' % (", ".join(["?" for x in range(8)]))
            self.conn.executemany(insert_legs_stmt, connection_list)
            self.parameters["target_list"] += (str(target_stop) + ",")
            self.conn.commit()

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
        self.add_fastest_path_column()
        self.add_time_to_prev_journey_fp_column()
        self.add_time_measures_to_journey()

    def get_stop_pairs(self):
        cur = self.conn.cursor()
        cur.execute('SELECT from_stop_I, to_stop_I FROM journeys GROUP BY from_stop_I, to_stop_I')
        return cur.fetchall()

    def get_targets(self):
        cur = self.conn.cursor()
        cur.execute('SELECT to_stop_I FROM journeys GROUP BY to_stop_I')
        return cur.fetchall()

    def get_origins(self):
        cur = self.conn.cursor()
        cur.execute('SELECT from_stop_I FROM journeys GROUP BY from_stop_I')
        return cur.fetchall()

    @timeit
    def add_fastest_path_column(self):
        print("adding fastest path column")
        if not self.targets:
            self.targets = self.get_targets()
        if not self.origins:
            self.origins = self.get_origins()
        cur = self.conn.cursor()
        for target in self.targets:
            fastest_path_journey_ids = []
            for origin in self.origins:
                cur.execute('SELECT dep_time, arr_time, journey_id FROM journeys '
                            'WHERE from_stop_I = ? AND to_stop_I = ? '
                            'ORDER BY dep_time ASC', (origin[0], target[0]))
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
        if not self.targets:
            self.get_targets()
        cur = self.conn.cursor()
        for target in self.targets:

            cur.execute('SELECT journey_id, from_stop_I, to_stop_I, dep_time FROM journeys '
                        'WHERE fastest_path = 1 AND to_stop_I = ? '
                        'ORDER BY from_stop_I, to_stop_I, dep_time ', (target[0],))

            all_trips = cur.fetchall()
            time_to_prev_journey = []
            prev_dep_time = None
            prev_origin = None
            prev_destination = None
            for trip in all_trips:
                journey_id = trip[0]
                from_stop_I = trip[1]
                to_stop_I = trip[2]
                dep_time = trip[3]
                if prev_origin != from_stop_I or prev_destination != to_stop_I:
                    prev_dep_time = None
                if prev_dep_time:
                    time_to_prev_journey.append((dep_time - prev_dep_time, journey_id))
                prev_origin = from_stop_I
                prev_destination = to_stop_I
                prev_dep_time = dep_time
            cur.executemany("UPDATE journeys SET pre_journey_wait_fp = ? WHERE journey_id = ?", time_to_prev_journey)
        self.conn.commit()

    @timeit
    def add_time_measures_to_journey(self):
        print("adding journey components")
        cur = self.conn.cursor()
        cur.execute("UPDATE journeys SET travel_time = arr_time - dep_time")
        cur.execute("UPDATE journeys "
                    "SET "
                    "in_vehicle_time = "
                    "(SELECT sum(arr_time - dep_time) AS in_vehicle_time FROM legs WHERE journeys.journey_id = legs.journey_id AND trip_I != -1 GROUP BY journey_id)")
        cur.execute("UPDATE journeys "
                    "SET "
                    "walking_time = "
                    "(SELECT sum(arr_time - dep_time) AS walking_time FROM legs WHERE journeys.journey_id = legs.journey_id AND trip_I = -1 GROUP BY journey_id) - transfer_wait_time")
        cur.execute("UPDATE journeys SET transfer_wait_time = travel_time - in_vehicle_time - walking_time")

        """
        in_vehicle_time
        transfer_wait_time
        walking_time
        """
        self.conn.commit()

    def initialize_database(self, journey_db_dir):
        assert not os.path.isfile(journey_db_dir)

        self.conn = sqlite3.connect(journey_db_dir)
        self._set_up_database()
        self._initialize_parameter_table()
        print("Database initialized!")
        if self.close_connection:
            self.conn.close()

    def _set_up_database(self):

        self.conn.execute('''CREATE TABLE IF NOT EXISTS parameters(
                     key TEXT UNIQUE,
                     value BLOB)''')
        if self.track_route:

            self.conn.execute('''CREATE TABLE IF NOT EXISTS journeys(
                         journey_id INTEGER PRIMARY KEY,
                         from_stop_I INT,
                         to_stop_I INT,
                         dep_time INT,
                         arr_time INT,
                         n_boardings INT,
                         movement_duration INT,
                         route TEXT,
                         travel_time INT,
                         pre_journey_wait_fp INT,
                         in_vehicle_time INT,
                         transfer_wait_time INT,
                         walking_time INT,
                         fastest_path INT)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS legs(
                         journey_id INT,
                         from_stop_I INT,
                         to_stop_I INT,
                         dep_time INT,
                         arr_time INT,
                         trip_I INT,
                         seq INT,
                         leg_stops TEXT)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS nodes(
                         stop_I INT,
                         agg_temp_distances INT,
                         agg_travel_time INT,
                         agg_boardings INT,
                         agg_transfer_wait INT,
                         agg_pre_journey_wait INT,
                         agg_walking_time INT)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS od_pairs(
                         from_stop_I INT,
                         to_stop_I INT,
                         avg_temp_distance INT,
                         agg_travel_time INT,
                         agg_boardings INT,
                         agg_transfer_wait INT,
                         agg_pre_journey_wait INT,
                         agg_walking_time INT)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS sections(
                         from_stop_I INT,
                         to_stop_I INT,
                         from_stop_pair_I INT,
                         to_stop_pair_I INT,
                         avg_temp_distance INT,
                         avg_travel_time INT,
                         n_trips INT)''')

            self.conn.execute('''CREATE TABLE IF NOT EXISTS transfer_nodes(
                         from_stop_I INT,
                         to_stop_I INT,
                         from_stop_pair_I INT,
                         to_stop_pair_I INT,
                         avg_waiting_time INT,
                         n_trips INT)''')

        else:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS journeys(
                         journey_id INTEGER PRIMARY KEY,
                         from_stop_I INT,
                         to_stop_I INT,
                         dep_time INT,
                         arr_time INT,
                         n_boardings INT,
                         time_to_prev_journey_fp INT,
                         fastest_path INT)''')

        self.conn.commit()

    def _initialize_parameter_table(self):

        parameters = Parameters(self.conn)

        parameters["multiple_targets"] = self.multitarget_routing
        parameters["gtfs_dir"] = self.gtfs_dir
        for param in ["location_name",
                      "lat_median",
                      "lon_median",
                      "start_time_ut",
                      "end_time_ut",
                      "start_date",
                      "end_date"]:
            parameters[param] = self.gtfs_meta[param]
        parameters["target_list"] = ","
        for key, value in self.routing_params.items():
            parameters[key] = value
        self.conn.commit()

        """
        Parameter table contents:
        GTFS db identification data:
        -city/feed = location_name
        -lon_median, lat_median
        -end_time_ut, end_date, start_date, start_time_ut
        -checksum?
        -db directory

        Routing parameters:
        -transfer_margin
        -walking speed
        -walking distance
        -time/date
        -multiple targets (true/false)
        -
        """

    def create_indicies(self):
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
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_jid ON journeys (journey_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_fid ON journeys (from_stop_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_tid ON journeys (to_stop_I)')
        if self.track_route:
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_jid ON legs (journey_id)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_trid ON legs (trip_I)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_fid ON legs (from_stop_I)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_legs_tid ON legs (to_stop_I)')
        self.conn.commit()
"""

    def add_fastest_path_column(self):
        cur = self.conn.cursor()
        # Select all distinct O-D pairs
        # For O-D pair in O-D pairs, create pareto-front
        cur.execute('SELECT from_stop_I, to_stop_I FROM journeys GROUP BY from_stop_I, to_stop_I')
        od_pairs = cur.fetchall()
        for pair in od_pairs:
            cur.execute('SELECT journey_id, arr_time, dep_time FROM journeys WHERE from_stop_I = ? AND to_stop_I = ? ORDER BY dep_time ASC', (pair[0], pair[1]))
            all_trips = cur.fetchall()
            pareto_trips = []
            cur_best_trips = []
            for trip in all_trips:
                is_dominated = False
                for best_trip in cur_best_trips:
                    if trip[1] > best_trip[1]:
                        is_dominated = True
                        break
                if is_dominated:
                    continue

                      """


class Parameters(object):
    """
    This provides dictionary protocol for updating parameters table, similar to GTFS metadata ("meta table").
    """

    def __init__(self, conn):
        self._conn = conn

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
        cur = self._conn.execute('SELECT key FROM metadata ORDER BY key')
        return cur

    def values(self):
        cur = self._conn.execute('SELECT value FROM metadata ORDER BY key')
        return cur



