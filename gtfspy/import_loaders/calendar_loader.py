from gtfspy.import_gtfs import decode_six
from gtfspy.import_loaders.table_loader import TableLoader


class CalendarLoader(TableLoader):
    fname = 'calendar.txt'
    table = 'calendar'
    tabledef = '(service_I INTEGER PRIMARY KEY, service_id TEXT UNIQUE NOT NULL, m INT, t INT, w INT, th INT, f INT, s INT, su INT, start_date TEXT, end_date TEXT)'
    copy_where = ("WHERE  date({start_ut}, 'unixepoch', 'localtime') < end_date "
                  "AND  start_date < date({end_ut}, 'unixepoch', 'localtime')")

    # service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
    # 1001_20150810_20151014_Ke,0,0,1,0,0,0,0,20150810,20151014
    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                # print row
                start = row['start_date']
                end = row['end_date']
                yield dict(
                    service_id    = prefix + decode_six(row['service_id']),
                    m             = int(row['monday']),
                    t             = int(row['tuesday']),
                    w             = int(row['wednesday']),
                    th            = int(row['thursday']),
                    f             = int(row['friday']),
                    s             = int(row['saturday']),
                    su            = int(row['sunday']),
                    start_date    = '%s-%s-%s' % (start[:4], start[4:6], start[6:8]),
                    end_date      = '%s-%s-%s' % (end[:4], end[4:6], end[6:8]),
                )

    @classmethod
    def index(cls, cur):
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_svid ON calendar (service_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_s_e ON calendar (start_date, end_date)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_m  ON calendar (m)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_t  ON calendar (t)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_w  ON calendar (w)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_th ON calendar (th)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_f  ON calendar (f)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_s  ON calendar (s)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_calendar_su ON calendar (su)')