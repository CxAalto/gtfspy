import contextlib
import csv
from datetime import datetime, date
import glob
import os
from pprint import pprint
import sqlite3
import time


name_map = dict(
 drive_time='ajoaika',
 drive_type='ajtyyppi',
 area_dep_time='aluelahtoaika',   # same as below but without ':'
 area_dep_time_time='aluelahtoaika_time',
 area_arr_time='aluetuloaika', # same as below but without ':'
 area_arr_time_time='aluetuloaika_time',
 bus_type='bussityyppi',
 sch_pass_by_time='joreohitusaika',             # scheduled time
 sch_pass_by_time_time='joreohitusaika_time',
 public_transport_type='joukkollaji',
 sign_date='kirjauspvm',   # a date
 k_day='kpaiva',
 cumul_stop_time='kumul_pysakkiaika',
 cumul_stop_area_time='kumul_pysakkialueella_oloaika',
 dep_time='lahtoaika',
 dep_time_time='lahtoaika_time',
 run_code='lahtokoodi',
 prev_stop='lahtopysakki',
 dtime='laika',
 run_sch_starttime='laikajore',  # 'HHMM' format, can wrap around at midnight (use 'day')
 sch_day='liikpaiva',            # the day before if after midnight
 line='linja',   # no letter suffix
 change_time='muutosaika',
 changer='muuttaja',
 pass_time='ohitusaika',
 delay='ohitusaika_ero',    # diff from schedule (actual-schedule)
 pass_time_time='ohitusaika_time',
 service_provider='palveluntuottaja',
 ptype='ptyyppi',
 ptypemove='ptyyppiliik',
 n_stops_made='pysahdyskpl',  # no of stops made
 stop_time='pysakkiaika',    # seconds at stop
 stop_time_presence_time='pysakkialueella_oloaika',
 stop_order='pysakkijarj',   # sequence number along this run
 stop_type='pysakkityyppi',
 route_id='reitti',   # can have letter suffix.  Join this with GTFS route.route_id column.
 dir='suunta',
 ta_month='ta_kuukausi',
 ta_week='ta_viikko',
 ta_year='ta_vuosi',
 day='tapahtumapaiva',     # the actual day of the corresponding times (hopefully)
 specificiation='tarkenne',
 arr_time='tuloaika',
 arr_time_time='tuloaika_time',
 this_stop='tulopysakki',
 error_step='virhe_askellus',
 error_gps='virhe_gps',
 error_start='virhe_lahto',
 error_passtime='virhe_ohitusaika',
 error_stop='virhe_pysakki',
 error_code='virhekoodi',
 yearweek='vuosiviikko',
)

relevant_fields = ['line', 'route_id', 'this_stop', 'run_code', 'run_sch_starttime',
                   'day', 'sch_day',
                   'arr_time_time','dep_time_time', 'sch_pass_by_time_time',
                   'stop_order', 'stop_time',
                   'delay',
                   'area_arr_time_time', 'area_dep_time_time',
                   ]
#data_types = dict(
#    line=(int, 'INTEGER'),
#    route=(str, 'STR'),
#    arr_time_time=(),
#    dep_time_time=,
#    sch_pass_by_time_time=,
#    stop_order=,
#    stopnumber=,
#    stop_time=,
#    dep_stop=,
#    day=,
#)

def to_unxitime(dt):
    return time.mktime(dt.timetuple())

def dict_factory(cursor, row):
    """sqlite helper: returned rows are dicts instead of tuples."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def make_schema(conn):
    """Set the schema on the database.

    This does not load any data, but just creates the schema
    (metadata).  This can be run multile times without removing or
    changing data - it just adds to the schema what is not already
    there.

    delete: if true, remove db before loading.
    """
    c = conn.cursor()
    c.execute('''
CREATE TABLE IF NOT EXISTS gps
( tnid INTEGER PRIMARY KEY,
  route_id TEXT,
  stop_id TEXT,
  run_code TEXT,
  run_sch_starttime unixtime,
  sch_time unixtime,
  arr_time unixtime,
  dep_time unixtime,
  arr_time_hour INT,
  stop_order INT,
  delay INT,
  shape_break INT)
''')

# Unused columns
#  line INT,
#  sch_day date,
#  day date,
#  stop_time INT,
#  area_arr_time unixtime,
#  area_dep_time unixtime,
#  arr_time_ts timestamp,
    c.execute('''
CREATE TABLE IF NOT EXISTS gps_meta
( run_code TEXT,
  run_sch_starttime unixtime,
  route_id TEXT,
  shape_id TEXT)
''')
    conn.commit()


def index(conn):
    """Make all indexes"""
    c = conn.cursor()
    # gps
    c.execute('CREATE INDEX IF NOT EXISTS idx_gps_t_stop ON gps '
                  '(dep_time, stop_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_gps_rc_rst ON gps '
                  '(run_code, run_sch_starttime);')
    c.execute('CREATE INDEX IF NOT EXISTS idx_gps_sid_t ON gps '
                  '(stop_id, arr_time)')

    # gps_meta
    c.execute('CREATE INDEX IF NOT EXISTS idx_gpsmeta_rc_rst ON gps_meta '
                  '(run_code, run_sch_starttime)')

    #conn.commit()



class gen_file_rows(object):
  def __init__(self, fname):
      self.fname = fname
      self.run_keys = set()
  def __iter__(self):
    """Get an iterator over CSV file

    This fuction iteratos over a CSV file, transformig it into dicts
    of only the relevant data, converted to proper Python types that we need.
    """
    rdr = csv.DictReader(open(self.fname))
    try:
      for i, row in enumerate(rdr):
        #print row.keys()
        #pprint(row)
        new_row = dict((name, row[name_map[name]]) for name in name_map )
        #pprint(new_row)


        #data_dict = dict((name, row[name_map[name]])
        #                 for name in relevant_fields)
        #print(data_dict)

        data = { }
        #data['line'] = int(new_row['line'])
        data['route_id'] = new_row['route_id']
        data['stop_id'] = int(new_row['this_stop'])

        data['stop_order'] = int(new_row['stop_order'])
#        data['day'] = datetime.strptime(new_row['day'], '%Y-%m-%d').date()
#        data['sch_day'] = datetime.strptime(new_row['sch_day'], '%Y-%m-%d').date()
        data['run_code'] = new_row['run_code']
        r_sch_st = int(new_row['run_sch_starttime'])
        if r_sch_st > 3000:
            raise ValueError("Suspicious value of run_sch_starttime: %s", new_row['run_sch_starttime'])
        # TODO: Does ajoaika_gps use "seconds since 12 hours before midnight", or is this real time?
        if r_sch_st >= 2400:
            # Rolls over to next day.  Must use manual math to process
            # it.  If we used this method for all times, then we would
            # get messed up on daylight saving time change days.
            # Here, we use 23:59 on that day, then add 60 seconds,
            # plus the number of seconds after 24:00.
            data['run_sch_starttime'] = \
              int(time.mktime(time.strptime(new_row['day']+' 2359', '%Y-%m-%d %H%M'))
              + ((r_sch_st//100)-24) * 60 + (r_sch_st%100) + 60 )
        else:
            # Same-day, use TZ
            data['run_sch_starttime'] = \
              int(time.mktime(time.strptime(new_row['day']+' '+"%04d"%(
                                            int(new_row['run_sch_starttime'])), '%Y-%m-%d %H%M')))
        self.run_keys.add((new_row['run_code'],
                           data['run_sch_starttime'],
                           new_row['route_id']))

        data['arr_time'] = int(time.mktime(time.strptime(
            new_row['day']+' '+new_row['arr_time_time'], '%Y-%m-%d %H:%M:%S')))
        data['dep_time'] = int(time.mktime(time.strptime(
            new_row['day']+' '+new_row['dep_time_time'], '%Y-%m-%d %H:%M:%S')))
        #data['stop_time'] = int(new_row['stop_time']) # sec at stop
        data['arr_time_hour'] = int(new_row['arr_time_time'][0:2])

        data['sch_time'] = int(time.mktime(time.strptime(
            new_row['day']+' '+new_row['sch_pass_by_time_time'], '%Y-%m-%d %H:%M:%S')))
        data['delay'] = int(new_row['delay'])

        #data['area_arr_time'] = int(time.mktime(time.strptime(
        #    new_row['day']+' '+new_row['area_arr_time_time'], '%Y-%m-%d %H:%M:%S')))
        #data['area_dep_time'] = int(time.mktime(time.strptime(
        #    new_row['day']+' '+new_row['area_dep_time_time'], '%Y-%m-%d %H:%M:%S')))

#        data['arr_time_ts'] = datetime.strptime(
#            new_row['day']+' '+new_row['arr_time_time'], '%Y-%m-%d %H:%M:%S')

        # This is not debugged yet.
        #if data['dep_time'] < data['arr_time']:
        #    raise ValueError("We have a wrap-around error")

        yield data
        #limit = 50
        #if limit and i >= limit-1:
        #    break
    except ValueError:
        print "-"*5 + 'exception' + "-"*5
        print i
        print row
        print data
        raise


def load_file(conn, fname, limit=None):
    """Load a single file into the DB.

    This is the core insertion function.
    """
    print "Loading file %s"%fname
    fields = next(iter(gen_file_rows(fname)))

    stmt = '''INSERT INTO gps (%s) VALUES (%s)'''%(
        (', '.join(fields.keys())),
        (', '.join(":"+x for x in fields.keys()))
        )

    row_iterator = gen_file_rows(fname)
    conn.executemany(stmt, row_iterator)
    conn.commit()

    stmt = ('INSERT INTO gps_meta (run_code, run_sch_starttime, route_id)'
            'VALUES (?, ?, ?)')
    conn.executemany(stmt, row_iterator.run_keys)


def load_all_data(conn, patterns=('scratch/ajoaika_gps/*.csv',), limit=None):
    """Load all files to DB

    While loading, makes a .tmp DB and moves it after it is done.
    This is a hack that changes a global variable - beware!
    """
    files = set()
    for pattern in patterns:
        files.update(glob.glob(pattern))
    files = sorted(files)

    for i, fname in enumerate(files):
        load_file(conn, fname)
        if limit and i >= limit-1:
            break

def calculate_shapes(conn):
    """Pre-compute the shapes corresponding to each run."""
    import shapes

    cur0 = conn.cursor()
    cur = conn.cursor()
    breakpoints_cache = { }

    cur0.execute('SELECT run_code, run_sch_starttime, route_id '
                'FROM gps_meta')
    for run_code, run_sch_starttime, route_id in cur0:

        # Get the stop points
        cur.execute('''SELECT stop_id, stop_order, lat, lon, sch_time, stop_I
                       FROM gps JOIN stops USING (stop_id)
                       WHERE run_code=? AND run_sch_starttime=?
                       ORDER BY stop_order''',
                    (run_code, run_sch_starttime))
        #print '%20s, %s'%(run_code, datetime.fromtimestamp(run_sch_starttime))

        stop_points = [ dict(stopid=row[0],
                             seq=row[1],
                             lat=row[2],
                             lon=row[3],
                             stop_I=row[5])
                        for row in cur]

        breakpoints, badness, shape_points, shape_id \
           = shapes.find_best_segments(cur,
                stop_points,
                shape_ids=None,
                route_id=route_id,
                breakpoints_cache=breakpoints_cache,
                cache_id=route_id)
        if len(breakpoints) == 0:
            # No valid route could be identified.
            continue
        # shape is the best shape.
        cur.execute('UPDATE gps_meta SET shape_id=? '
                    'WHERE run_code=? AND run_sch_starttime=?',
                    (shape_id, run_code, run_sch_starttime))
        # breakpoints is the corresponding points.  insert this into gps.
        assert len(breakpoints) == len(stop_points)
        cur.executemany('UPDATE gps SET shape_break=? '
                        'WHERE run_code=? AND run_sch_starttime=? '
                        '      AND stop_order=?',
                    ((bkpt, run_code, run_sch_starttime, stpt['seq'])
                     for bkpt, stpt in zip(breakpoints, stop_points) ))
    conn.commit()



if __name__ == "__main__":
    import sys
    cmd = sys.argv[1]
    DB_NAME = sys.argv[2]
    INPUT_PATTERNS = sys.argv[3:]
    #assert len(sys.argv) == 4, "Arguments: cmd DB_NAME input_pattern"

    #DB_FNAME = '/local/cache/hsl_data/db-all.sqlite'
    #DB_FNAME = '/local/cache/hsl_data/db-7day.sqlite'

    #conn = sqlite3.connect(DB_FNAME)
    import db
    conn = db.connect_gps(DB_NAME, mode='w')
    if 'import' in cmd:
        print "Importing"
        conn.execute('PRAGMA page_size = 4096')
        conn.execute('PRAGMA journal_mode = OFF;')
        conn.execute('PRAGMA synchronous = OFF;')
        make_schema(conn)
        load_all_data(conn, INPUT_PATTERNS, limit=None)
    if 'index' in cmd:
        print "Indexing"
        index(conn)
    if 'shape' in cmd:
        conn = db.connect_gps(DB_NAME, mode='w', gtfs='hsl-2015-04-24')
        print "Importing shapes"
        calculate_shapes(conn)
