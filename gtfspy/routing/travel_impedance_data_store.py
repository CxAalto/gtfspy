import sqlite3
import pandas as pd


class TravelImpedanceDataStore:

    def __init__(self, db_fname, timeout=100):
        self.db_fname = db_fname
        self.timeout = timeout
        self.conn = sqlite3.connect(self.db_fname, timeout)

    def read_data_as_dataframe(self,
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

    def create_table(self, travel_impedance_measure):
        print("Creating table: ", travel_impedance_measure)
        sql = "CREATE TABLE IF NOT EXISTS " + travel_impedance_measure + " (from_stop_I INT, " \
            "to_stop_I INT, " \
            "min REAL, " \
            "max REAL, " \
            "median REAL, " \
            "mean REAL, " \
            "UNIQUE (from_stop_I, to_stop_I) )"
        self.conn.execute(sql)

    def create_indices_for_tables(self):
        for travel_impedance_measure in self.travel_impedance_measure_names:
            self._create_index_for_travel_impedance_measure_table(travel_impedance_measure)

    def create_index(self, travel_impedance_measure_name):
        table = travel_impedance_measure_name
        sql_from = "CREATE INDEX IF NOT EXISTS " + table + "_from_stop_I ON " + table + " (from_stop_I)"
        sql_to = "CREATE INDEX IF NOT EXISTS " + table + "_to_stop_I ON " + table + " (to_stop_I)"
        self.conn.execute(sql_from)
        self.conn.execute(sql_to)
        self.conn.commit()

    def insert_data(self, travel_impedance_measure_name, data):
        """
        Parameters
        ----------
        travel_impedance_measure_name: str
        data: list[dict]
            Each list element must contain keys:
            "from_stop_I", "to_stop_I", "min", "max", "median" and "mean"
        """
        f = float
        data_tuple = [(x["from_stop_I"], x["to_stop_I"], f(x["min"]), f(x["max"]), f(x["median"]), f(x["mean"])) for
                      x in data]
        insert_stmt = '''INSERT OR REPLACE INTO ''' + travel_impedance_measure_name + ''' (
                              from_stop_I,
                              to_stop_I,
                              min,
                              max,
                              median,
                              mean) VALUES (?, ?, ?, ?, ?, ?) '''
        self.conn.executemany(insert_stmt, data_tuple)
        self.conn.commit()

    # def apply_insertion_speedups(self):
    #     self.conn.execute("PRAGMA SYNCHRONOUS = OFF")



