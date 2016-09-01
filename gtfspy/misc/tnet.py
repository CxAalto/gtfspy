from __future__ import absolute_import

import db


def make_tnet():
    conn = db.get_db()
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    cur4 = conn.cursor()

    cur.execute('''DROP TABLE IF EXISTS tnet_actual''')
    cur.execute('''CREATE TABLE IF NOT EXISTS tnet_actual
(
tnid1 INT, tnid2 INT,
stop1 INT, time1 unixtime,
stop2 INT, time2 unixtime,
type INT
)
''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tneta_tnid1 ON tnet_actual (tnid1);')
    cur.execute('CREATE INDEX idx_tneta_s1_t1 ON tnet_actual (stop1, time1);')
    conn.commit()



    # Find our IDs that are relevant.
    cur.execute('''select distinct run_code, run_sch_starttime
                   from gps
                   -- where ?<=run_sch_starttime and run_sch_starttime < ?
                   order by run_sch_starttime''',
                   #(start, end)
                )


    for run_code, run_sch_starttime in cur:

        # Below we process one bus trip.
        cur2.execute('''select tnid, arr_time, dep_time, stop_id
                        FROM gps
                        WHERE run_code=? and run_sch_starttime=?
                        ORDER BY stop_order''',
                     (run_code, run_sch_starttime))

        last_tnid, last_stop_id, last_time = None, None, None
        # For each stop in this bus trip.
        for tnid, arr_time, dep_time, stop_id in cur2:
            # Create a link along the bus travel path.
            if last_stop_id is not None:
                cur3.execute('''INSERT INTO tnet_actual VALUES
                             (?, ?, ?, ?, ?, ?, 0)''',
                             (last_tnid, tnid,
                              last_stop_id, last_time,
                              stop_id, arr_time))
                #conn.commit()
                #print "Continuing: "
            ## Find walking transfers.
            ## ex point: vsg.sid=9040230
            #cur4.execute('''SELECT tnid, stop_id, arr_time
            #              FROM view_stop_groups vsg JOIN gps ON (vsg.other_sid=gps.stop_id)
            #              -- LEFT JOIN stop s ON (gps.stop_id=s.sid)
            #              WHERE vsg.sid=? AND ?<arr_time AND arr_time<?''',
            #              (stop_id, arr_time+60, arr_time+600))
            #for next_tnid, next_stop_id, next_arr_time in cur4:
            #    print "transfer:", next_arr_time - arr_time
            #
            #    cur3.execute('''INSERT INTO tnet_actual VALUES
            #                 (?, ?, ?, ?, ?, ?, 1)''',
            #                 (tnid, next_tnid,
            #                  stop_id, arr_time,
            #                  next_stop_id, next_arr_time))
            #    #conn.commit()

            # Compute using stop distances
            max_distance = 200
            max_time = 600

            cur4.execute('''SELECT tnid, stop_id, arr_time, distance
                          FROM stop_distances sd JOIN gps ON (sd.sid2=gps.stop_id)
                          -- LEFT JOIN stop s ON (gps.stop_id=s.sid)
                          WHERE sd.sid1=? AND distance < ?
                              AND (?+60+distance)<arr_time AND arr_time<(?+?)''',
                          (stop_id, max_distance, arr_time, arr_time, max_time))
            for next_tnid, next_stop_id, next_arr_time, distance in cur4:
                #print "transfer:", next_arr_time - arr_time, distance

                cur3.execute('''INSERT INTO tnet_actual VALUES
                             (?, ?, ?, ?, ?, ?, 1)''',
                             (tnid, next_tnid,
                              stop_id, arr_time,
                              next_stop_id, next_arr_time))


            last_tnid = tnid
            last_stop_id = stop_id
            last_time = arr_time

    conn.commit()

if __name__ == "__main__":
    make_tnet()
