import pandas

from gtfspy.import_loaders.table_loader import TableLoader, decode_six

class CalendarDatesLoader(TableLoader):
    fname = 'calendar_dates.txt'
    table = 'calendar_dates'
    tabledef = '(service_I INTEGER NOT NULL, date TEXT, exception_type INT)'
    copy_where = ("WHERE  date({start_ut}, 'unixepoch', 'localtime') <= date "
                  "AND  date < date({end_ut}, 'unixepoch', 'localtime')")

    def gen_rows(self, readers, prefixes):
        conn = self._conn
        cur = conn.cursor()
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                date = row['date']
                date_str = '%s-%s-%s' % (date[:4], date[4:6], date[6:8])
                service_id = prefix+row['service_id']
                # We need to find the service_I of this.  To do this we
                # need to check the calendar table, since that (and only
                # that) is the absolute list of service_ids.
                service_I = cur.execute(
                    'SELECT service_I FROM calendar WHERE service_id=?',
                    (decode_six(service_id),)).fetchone()
                if service_I is None:
                    # We have to add a new fake row in order to get a
                    # service_I.  calendar is *the* authoritative source
                    # for service_I:s.
                    cur.execute('INSERT INTO calendar '
                                '(service_id, m,t,w,th,f,s,su, start_date,end_date)'
                                'VALUES (?, 0,0,0,0,0,0,0, ?,?)',
                                (decode_six(service_id), date_str, date_str)
                                )
                    service_I = cur.execute(
                        'SELECT service_I FROM calendar WHERE service_id=?',
                        (decode_six(service_id),)).fetchone()
                service_I = service_I[0]  # row tuple -> int

                yield dict(
                    service_I     = int(service_I),
                    date          = date_str,
                    exception_type= int(row['exception_type']),
                )

    def post_import(self, conn):
        update_calendar_table_exceptions_only_entries(conn)

def update_calendar_table_exceptions_only_entries(conn):
    # Exceptions do not run on any weekdays (m=0, t=0, w=0, th=0, f=0, s=0, and su=0)
    exceptions_only_entries = pandas.read_sql("SELECT service_I FROM calendar WHERE m=0 AND t=0 AND w=0 AND th=0 AND f=0 AND s=0 AND su=0;", conn)
    print(exceptions_only_entries)
    for service_I in exceptions_only_entries['service_I'].values:
        service_I = int(service_I)
        conn.execute("UPDATE calendar "
                       "SET start_date=(SELECT min(date) FROM calendar_dates WHERE service_I=?), "
                            "end_date=(SELECT max(date) FROM calendar_dates WHERE service_I=?) WHERE calendar.service_I=?",
                       (service_I, service_I, service_I))



