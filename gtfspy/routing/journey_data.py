import sqlite3
import os
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.label import LabelTimeBoardingsAndRoute
from gtfspy.routing.models import Connection
from gtfspy.util import timeit
from gtfspy.gtfs import GTFS

# TODO: DB-handling: make sure the connection is closed using __enter__, __exit__?
# TODO: Find out how to determine if this is a fastest path or a less boardings path


class JourneyDataManager:
    @timeit
    def __init__(self,
                 journey_db_dir,
                 target_stops,
                 gtfs_dir,
                 routing_params
                 ):
        """

        :param gtfs: GTFS object
        :param list_of_stop_profiles: dict of NodeProfileMultiObjective
        :param target_stops: list
        """

        self.routing_params = routing_params
        self.target_stops = target_stops
        self.gtfs_dir = gtfs_dir
        self.gtfs_meta = GTFS(self.gtfs_dir).meta
        self.parameters = Parameters(self.conn)

        initialize = False
        if not os.path.isfile(journey_db_dir):
            initialize = True
        self.conn = sqlite3.connect(journey_db_dir)
        if initialize:
            self._set_up_tables()
            self._initialize_parameter_table()
        else:
            self._check_that_dbs_match()

    def __del__(self):
        self.conn.close()

    def import_journey_data(self, list_of_stop_profiles):
        cur = self.conn.cursor()

        cur.execute('PRAGMA synchronous = OFF;')
        if not isinstance(list_of_stop_profiles, list):
            list_of_stop_profiles = [list_of_stop_profiles]

        self._insert_journeys_into_db(list_of_stop_profiles)
        self._create_indicies()
        self.conn.commit()
        # Next 3 lines are python 3.6 work-arounds again.
        self.conn.isolation_level = None  # former default of autocommit mode
        cur.execute('VACUUM;')
        self.conn.isolation_level = ''  # back to python default
        # end python3.6 workaround
        print("Analyzing...")
        cur.execute('ANALYZE')
        self.conn.close()

        print("Finished import process")

    def _set_up_tables(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS journeys(
                     journey_id INTEGER PRIMARY KEY,
                     from_stop_I INT,
                     to_stop_I INT,
                     dep_time INT,
                     arr_time INT,
                     n_boardings INT)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS connections(
                     journey_id INT,
                     from_stop_I INT,
                     to_stop_I INT,
                     dep_time INT,
                     arr_time INT,
                     trip_I INT,
                     seq INT)''')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS parameters(
                     key TEXT UNIQUE,
                     value BLOB)''')

    def _initialize_parameter_table(self):
        self.parameters["multiple_targets"] = True if len(self.target_stops) > 1 else False
        self.parameters["gtfs_dir"] = self.gtfs_dir
        for param in ["location_name",
                      "lat_median",
                      "lon_median",
                      "start_time_ut",
                      "end_time_ut",
                      "start_date",
                      "end_date"]:
            self.parameters[param] = self.gtfs_meta[param]

        for key, value in self.routing_params:
            self.parameters[key] = value

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

    def _check_that_dbs_match(self):
        for key, value in self.parameters:
            if key in self.gtfs_meta.keys():
                assert self.gtfs_meta[key] == value

    def _check_last_journey_id(self):
        cur = self.conn.cursor()
        val = cur.execute("select max(journey_id) FROM journeys").fetchone()
        return val[0]

    def _insert_journeys_into_db(self, list_of_stop_profiles):
        print("Collecting journey and connection data")
        journey_id = self._check_last_journey_id()+1
        journey_list = []
        connection_list = []
        for stop_profiles in list_of_stop_profiles:
            tot = len(stop_profiles)
            for i, origin_stop in enumerate(stop_profiles, start=1):
                print("\r Stop " + str(i) + " of " + str(tot), end='', flush=True)

                assert (isinstance(stop_profiles[origin_stop], NodeProfileMultiObjective))

                for label in stop_profiles[origin_stop].get_final_optimal_labels():
                    assert (isinstance(label, LabelTimeBoardingsAndRoute))
                    # We need to "unpack" the journey to actually figure out where the trip went
                    # (there can be several targets).

                    target_stop, new_connection_values = self._collect_connection_data(journey_id, label)
                    values = [journey_id,
                              origin_stop,
                              target_stop,
                              int(label.departure_time),
                              int(label.arrival_time_target),
                              label.n_boardings]

                    journey_list.append(values)
                    connection_list += new_connection_values
                    journey_id += 1

            print()
            print("Inserting journeys into database")
            insert_journeys_stmt = '''INSERT INTO journeys(
                  journey_id,
                  from_stop_I,
                  to_stop_I,
                  dep_time,
                  arr_time,
                  n_boardings) VALUES (%s) ''' % (", ".join(["?" for x in range(6)]))
            self.conn.executemany(insert_journeys_stmt, journey_list)

            print("Inserting connections into database")
            insert_connections_stmt = '''INSERT INTO connections(
                                  journey_id,
                                  from_stop_I,
                                  to_stop_I,
                                  dep_time,
                                  arr_time,
                                  trip_id,
                                  seq) VALUES (%s) ''' % (", ".join(["?" for x in range(7)]))
            self.conn.executemany(insert_connections_stmt, connection_list)

    def _collect_connection_data(self, journey_id, label):
        target_stop = None
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
                    int(trip_id),
                    int(seq)
                )

                value_list.append(values)
                seq += 1
                target_stop = connection.arrival_stop
            if not cur_label.previous_label:
                break
            cur_label = cur_label.previous_label

        return target_stop, value_list

    def _create_indicies(self):
        print("Indexing")
        cur = self.conn.cursor()
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_jid ON journeys (journey_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_fid ON journeys (from_stop_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_journeys_tid ON journeys (to_stop_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_connections_jid ON connections (journey_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_connections_trid ON connections (trip_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_connections_fid ON connections (from_stop_I)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_connections_tid ON connections (to_stop_I)')


class Parameters(object):
    """
    This provides dictionary protocol for updating parameters table, similar to GTFS metadata ("meta table").
    """

    def __init__(self, conn):
        self._conn = conn

    def __setitem__(self, key, value):
        self._conn.execute("INSERT OR REPLACE INTO parameters('key', 'value') VALUES (?, ?)", parameters=(key, value))
        self._conn.commit()

    def __getitem__(self, key):
        cur = self._conn.cursor()
        cur.execute("SELECT 'value' FROM parameters WHERE 'key'=?", parameters=(key,))
        val = cur.fetchone()
        if not val:
            raise KeyError("This journey db does not have parameter: %s" % key)
        return val[0]

    def __delitem__(self, key):
        self._conn.execute("DELETE FROM parameters WHERE 'key'=?", parameters=(key,))
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



