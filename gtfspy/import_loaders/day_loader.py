from datetime import timedelta, datetime

from gtfspy.import_loaders.table_loader import TableLoader


class DayLoader(TableLoader):
    # Note: calendar and calendar_dates should have been imported before
    # importing with DayLoader
    fname = None
    table = "days"
    tabledef = "(date TEXT, day_start_ut INT, trip_I INT)"
    copy_where = "WHERE  {start_ut} <= day_start_ut  AND  day_start_ut < {end_ut}"

    def post_import(self, cur):
        insert_data_to_days(cur, self._conn)

    def index(self, cur):
        create_day_table_indices(cur)


def create_day_table_indices(cursor):
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_days_day ON days (date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_days_dsut_tid ON days (day_start_ut, trip_I)")


def drop_day_table_indices(cursor):
    cursor.execute("DROP INDEX IF EXISTS idx_days_day")
    cursor.execute("DROP INDEX IF EXISTS idx_days_dsut_tid")


def insert_data_to_days(cur, conn):
    # clear if something existed before
    cur.execute("DELETE FROM days")
    days = []
    # This index is important here, but no where else, and not for
    # future processing.  So, create it here, delete it at the end
    # of the function.  If this index was important, it could be
    # moved to CalendarDatesLoader.
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calendar_dates_sid ON calendar_dates (service_I)")

    cur.execute("SELECT * FROM calendar")
    colnames = cur.description
    cur2 = conn.cursor()

    def make_dict(row):
        """Quick function to make dictionary out of row"""
        return dict((key[0], value) for key, value in zip(colnames, row))

    def iter_dates(start, end):
        """Iter date objects for start--end, INCLUSIVE of end date"""
        one_day = timedelta(days=1)
        date = start
        while date <= end:
            yield date
            date += one_day

    weekdays = ["m", "t", "w", "th", "f", "s", "su"]
    # For every row in the calendar...
    for row in cur:
        row = make_dict(row)
        service_I = int(row["service_I"])
        # EXCEPTIONS (calendar_dates): Get a set of all
        # exceptional days.  exception_type=2 means that service
        # removed on that day.  Below, we will exclude all dates
        # that are in this set.
        cur2.execute(
            "SELECT date FROM calendar_dates " "WHERE service_I=? and exception_type=?",
            (service_I, 2),
        )
        exception_dates = set(x[0] for x in cur2.fetchall())
        #
        start_date = datetime.strptime(row["start_date"], "%Y-%m-%d").date()
        end_date = datetime.strptime(row["end_date"], "%Y-%m-%d").date()
        # For every date in that row's date range...
        for date in iter_dates(start_date, end_date):
            weekday = date.isoweekday() - 1  # -1 to match weekdays list above
            # Exclude dates with service exceptions
            date_str = date.strftime("%Y-%m-%d")
            if date_str in exception_dates:
                # print "calendar_dates.txt exception: removing %s from %s"%(service_I, date)
                continue
            # If this weekday is marked as true in the calendar...
            if row[weekdays[weekday]]:
                # Make a list of this service ID being active then.
                days.append((date, service_I))

    # Store in database, day_start_ut is "noon minus 12 hours".
    cur.executemany(
        """INSERT INTO days
                   (date, day_start_ut, trip_I)
                   SELECT ?, strftime('%s', ?, '12:00', 'utc')-43200, trip_I
                   FROM trips WHERE service_I=?
                   """,
        ((date, date, service_I) for date, service_I in days),
    )

    # EXCEPTIONS: Add in dates with exceptions.  Find them and
    # store them directly in the database.
    cur2.execute(
        "INSERT INTO days "
        "(date, day_start_ut, trip_I) "
        "SELECT date, strftime('%s',date,'12:00','utc')-43200, trip_I "
        "FROM trips "
        "JOIN calendar_dates USING(service_I) "
        "WHERE exception_type=?",
        (1,),
    )
    conn.commit()
    cur.execute("DROP INDEX IF EXISTS main.idx_calendar_dates_sid")


def recreate_days_table(conn):
    cursor = conn.cursor()
    drop_day_table_indices(cursor)
    insert_data_to_days(cursor, conn)
    create_day_table_indices(cursor)
