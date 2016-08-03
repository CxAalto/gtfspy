

import db


def test_stops():
    conn = db.connect_gps('2015-03-01', gtfs='hsl-2015-04-24')
    cur = conn.cursor()

    # sid INTEGER PRIMARY KEY,    - GTFS stop id
    # code TEXT,                  - actual code, such as "E2222"
    # name TEXT,                  - name
    # desc TEXT,                  - usually same as name
    # lat REAL,
    # lon REAL,
    # parent_id INT,              - parent stop ID (e.g. all stops in the same area)
    # location_type INT,          - unsure what this dose
    # accessible BOOL             - wheelchair accessible?

    # Iterate through all rows.  The available fields are above.
    # After running "execute" on the cursor, the cursor becomes an
    # iterator over all rows.
    cur.execute('SELECT stop_I, lat, lon FROM stops')
    for row in cur:
        print row
        break

    # Select lat and long for one stop, first by GTFS stopid and then
    # by stop code.
    cur.execute('SELECT lat, lon FROM stops WHERE stop_id=?', (2222227, ))
    lat, lon = cur.fetchone()
    print lat, lon

    cur.execute('SELECT lat, lon FROM stops WHERE code=?', ('E2222', ))
    lat, lon = cur.fetchone()
    print lat, lon



def test_get_traces():
    """Examples of getting all data"""
    conn = db.connect_gps('2015-03-01', gtfs='hsl-2015-04-24')
    cur = conn.cursor()

    # This iterates over all distinct (run_code, run_sch_starttime)
    # pairs, which should be sufficient to identify all unique runs.
    cur.execute('''select distinct run_code, run_sch_starttime as "run_sch_starttime [unixtime]" from gps''')
    for row in cur:
        run_code, run_sch_starttime = row
        break
    print run_code, run_sch_starttime

    # Example of getting all times and delays of one bus.
    cur.execute('''select arr_time, delay from gps WHERE run_code=? and run_sch_starttime=?''',
                (run_code, run_sch_starttime))
    print cur.fetchall()


    # Example of getting all times and delays of one bus, plus stop locations
    cur.execute('''select arr_time, delay, lat, lon
                   FROM gps LEFT JOIN stops using (stop_id)
                   WHERE run_code=? and run_sch_starttime=?
                   ORDER BY sch_time''',
                (run_code, run_sch_starttime))
    for row in cur:
        print row
