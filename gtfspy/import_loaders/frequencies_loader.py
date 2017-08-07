import pandas

from gtfspy import util
from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class FrequenciesLoader(TableLoader):
    """Load the general frequency table."""
    fname = 'frequencies.txt'
    table = 'frequencies'

    tabledef = (u'(trip_I INT, '
                u'start_time TEXT, '
                u'end_time TEXT, '
                u'headway_secs INT,'
                u'exact_times INT, '
                u'start_time_ds INT, '
                u'end_time_ds INT'
                u')')
    extra_keys = [u'trip_I',
                  u'start_time_ds',
                  u'end_time_ds',
                  ]
    extra_values = [u'(SELECT trip_I FROM trips WHERE trip_id=:_trip_id )',
                    '(substr(:start_time,-8,2)*3600 + substr(:start_time,-5,2)*60 + substr(:start_time,-2))',
                    '(substr(:end_time,-8,2)*3600 + substr(:end_time,-5,2)*60 + substr(:end_time,-2))',
                    ]

    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                yield dict(
                    _trip_id=prefix + decode_six(row['trip_id']),
                    start_time=row['start_time'],
                    end_time=row['end_time'],
                    headway_secs=int(row['headway_secs']),
                    exact_times=int(row['exact_times']) if 'exact_times' in row and row['exact_times'].isdigit() else 0
                )

    def post_import(self, cur):
        # For each (start_time_dependent) trip_I in frequencies.txt
        conn = self._conn
        frequencies_df = pandas.read_sql("SELECT * FROM " + self.table, conn)

        for freq_tuple in frequencies_df.itertuples():
            trip_data = pandas.read_sql_query("SELECT * FROM trips WHERE trip_I= " + str(int(freq_tuple.trip_I)), conn)
            assert len(trip_data) == 1
            trip_data = list(trip_data.itertuples())[0]
            freq_start_time_ds = freq_tuple.start_time_ds
            freq_end_time_ds = freq_tuple.end_time_ds
            trip_duration = cur.execute("SELECT max(arr_time_ds) - min(dep_time_ds) "
                                        "FROM stop_times "
                                        "WHERE trip_I={trip_I}".format(trip_I=str(int(freq_tuple.trip_I)))
                                        ).fetchone()[0]
            if trip_duration is None:
                raise ValueError("Stop times for frequency trip " + trip_data.trip_id + " are not properly defined")
            headway = freq_tuple.headway_secs

            sql = "SELECT * FROM stop_times WHERE trip_I=" + str(trip_data.trip_I) + " ORDER BY seq"
            stop_time_data = pandas.read_sql_query(sql, conn)

            start_times_ds = range(freq_start_time_ds, freq_end_time_ds, headway)
            for i, start_time in enumerate(start_times_ds):
                trip_id = trip_data.trip_id + u"_freq_" + str(start_time)
                route_I = trip_data.route_I
                service_I = trip_data.service_I

                shape_id = trip_data.shape_id
                direction_id = trip_data.direction_id
                headsign = trip_data.headsign
                end_time_ds = start_time + trip_duration

                # insert these into trips
                query = "INSERT INTO trips (trip_id, route_I, service_I, shape_id, direction_id, " \
                            "headsign, start_time_ds, end_time_ds)" \
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

                params = [trip_id, int(route_I), int(service_I), shape_id, direction_id, headsign, int(start_time), int(end_time_ds)]
                cur.execute(query, params)

                query = "SELECT trip_I FROM trips WHERE trip_id='{trip_id}'".format(trip_id=trip_id)
                trip_I = cur.execute(query).fetchone()[0]

                # insert into stop_times
                # TODO! get the original data
                dep_times_ds = stop_time_data['dep_time_ds']
                dep_times_ds = dep_times_ds - min(dep_times_ds) + start_time
                arr_times_ds = stop_time_data['arr_time_ds']
                arr_times_ds = arr_times_ds - min(arr_times_ds) + start_time
                shape_breaks_series = stop_time_data['shape_break']
                stop_Is = stop_time_data['stop_I']

                shape_breaks = []
                for shape_break in shape_breaks_series:
                    value = None
                    try:
                        value = int(shape_break)
                    except Exception:
                        pass
                    shape_breaks.append(value)

                for seq, (dep_time_ds, arr_time_ds, shape_break, stop_I) in enumerate(zip(dep_times_ds,
                                                                                          arr_times_ds,
                                                                                          shape_breaks,
                                                                                          stop_Is)):
                    arr_time_hour = int(arr_time_ds // 3600)
                    query = "INSERT INTO stop_times (trip_I, stop_I, arr_time, " \
                            "dep_time, seq, arr_time_hour, shape_break, arr_time_ds, dep_time_ds) " \
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    arr_time = util.day_seconds_to_str_time(arr_time_ds)
                    dep_time = util.day_seconds_to_str_time(dep_time_ds)

                    cur.execute(query, (int(trip_I), int(stop_I), arr_time, dep_time, int(seq + 1),
                                        int(arr_time_hour), shape_break, int(arr_time_ds), int(dep_time_ds)))

        trip_Is = frequencies_df['trip_I'].unique()
        for trip_I in trip_Is:
            for table in ["trips", "stop_times"]:
                cur.execute("DELETE FROM {table} WHERE trip_I={trip_I}".format(table=table, trip_I=trip_I))
        self._conn.commit()