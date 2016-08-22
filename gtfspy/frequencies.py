from __future__ import absolute_import

import datetime
import json
import sys

import numpy as np

from . import db
from .gtfs import GTFS
from . import shapes

def to_json(x):
    return json.dumps(x)


# season = 'summer'/'winter'
# day = one of ['m', 't', 'w', 'th', 'f', 's', 'su']
# hour should be something between 03-28
def get_metainfo(season, day):
    """Get the GTFS DB and 'normal day' for frequency analysis."""
    daylist = ['m', 't', 'w', 'th', 'f', 's', 'su']

    # To prevent SQL injection:
    if day not in daylist:
        return {} # There's probably a smarter solution too...

    daynumber = daylist.index(day)

    # Summer schedules are defined according to week 3-9 August,
    # winter as the week 17-23 August
    # The schedules are standard on those weeks except for two
    # services: 3001T_20150710_20150809_M2 and
    # 3001T_20150710_20150809_T2
    if season == 's2015':
        dbname = 'scratch/db/hsl-2015-07-12.sqlite'
        monday = datetime.datetime(2015,8,3)
    elif season == 'w2015':
        dbname = 'scratch/db/hsl-2015-07-12.sqlite'
        monday = datetime.datetime(2015,8,17)
    elif season == 'w2014':
        dbname = 'scratch/db/hsl-2015-04-24.sqlite'
        monday = datetime.datetime(2015,5,13)

    return daynumber, dbname, monday, day



def get_stop_freqs(season, day, hour, return_dict=False):
    """Get stop frequencies at one stop."""
    daynumber, dbname, monday, weekday = get_metainfo(season, day)

    conn = GTFS(dbname).conn
    cur = conn.cursor()

    date = monday + datetime.timedelta(days=daynumber)
    datestr = date.strftime('%Y-%m-%d')

    # Find our IDs that are relevant.
    cur.execute('''SELECT stops.name, cnt, lat, lon, stop_id
                    FROM  (
                        SELECT stop_id, count(*) AS cnt FROM
                            calendar
                               LEFT JOIN trips USING (service_I)
                               JOIN stop_times USING (trip_I)
                               LEFT JOIN stops USING (stop_I)
                       WHERE %s==1 AND start_date<=? AND ?<=end_date
                           AND arr_time_hour=?
                       GROUP BY stop_I )
                    LEFT JOIN stops USING (stop_id)'''%(weekday),(datestr,datestr,int(hour)))

    if return_dict:
        all_stop_freqs = { }
        for row in cur:
            stop_id = row[4]
            stop_freqs = all_stop_freqs[stop_id] = { }
            stop_freqs['name'] = row[0]
            stop_freqs['freq'] = row[1]
            stop_freqs['lat'] = row[2]
            stop_freqs['lon'] = row[3]
        return all_stop_freqs

    all_stop_freqs = []
    for row in cur:
        stop_freqs = {}
        stop_freqs['name'] = row[0]
        stop_freqs['freq'] = row[1]
        stop_freqs['lat'] = row[2]
        stop_freqs['lon'] = row[3]

        all_stop_freqs.append(stop_freqs)

    return dict(all_stop_freqs=all_stop_freqs)


def diff_create(data1, data2):
    """Take diff between two dictionaries.

    Data from data2 is returned by default.  If a key is missing from
    data2, then it is added with the values from data1.  The 'freq' key
    is returned as freq2-freq1."""
    # data2 will be returned to the user.
    for key, value in data2.iteritems():
        if key in data1:
            value['freq'] -= data1[key]['freq']
        elif key not in data1:
            pass # nothing to do
    # Handle everything in data1 that was not in data2.  This mutates
    # data1, but since it will be discarded, that is no big deal.
    for key in data1:
        if key not in data2:
            data2[key] = data1[key]
            data2[key]['freq'] = - data1[key]['freq']
    return data2

def diff_stop_freqs(season1, day1, hour1, season2, day2, hour2):
    """Return change from time1 to time2.

    This is time2-time1, or the amount added to time1 to get time2.
    """
    data1 = get_stop_freqs(season1, day1, hour1, return_dict=True)
    data2 = get_stop_freqs(season2, day2, hour2, return_dict=True)

    data2 = diff_create(data1, data2)
    # Transform back to list
    all_stop_freqs = [row for row in data2.itervalues() ]
    return dict(all_stop_freqs=all_stop_freqs)






def get_pair_freqs(season, day, hour, return_dict=False, with_shapes=False):
    """Get stop frequencies between pairs of stops."""
    daynumber, dbname, monday, weekday = get_metainfo(season, day)

    conn = GTFS(dbname).conn
    cur = conn.cursor()

    # what is going on here?:
    date = monday + datetime.timedelta(days=daynumber)
    datestr = date.strftime('%Y-%m-%d')

    # Find our IDs that are relevant.
    cur.execute('''SELECT trip_I, cnt, seq1, seq2,
                          S1.code, S2.code,
                          S1.name AS name1, S2.name AS name2,
                          S1.lat, S1.lon, S2.lat, S2.lon
                   FROM ( SELECT st1.trip_I,  st1.seq AS seq1,  st2.seq AS seq2,
                              count(*) AS cnt,  st1.arr_time AS at1,
                              st1.stop_I AS sid1,   st2.stop_I AS sid2
                          FROM calendar LEFT JOIN trips USING (service_I)
                              JOIN  stop_times st1 ON (trips.trip_I=st1.trip_I)
                              JOIN stop_times st2 ON (st1.trip_I = st2.trip_I AND st1.seq = st2.seq-1)
                              LEFT JOIN trips USING (trip_I)
                          WHERE %s==1 AND start_date<=? AND ?<=end_date
                              AND st1.arr_time_hour=? GROUP BY sid1, sid2 )
                   LEFT JOIN stops S1 ON (sid1=S1.stop_I)
                   LEFT JOIN stops S2 ON (sid2=S2.stop_I)
                   --ORDER BY cnt DESC LIMIT 10 ;
               '''%(weekday), (datestr,datestr,int(hour)))


    cur2 = conn.cursor()
    def _lats_and_lons(row, sf, with_shapes):
        sf['lats'] = [row[8], row[10]]
        sf['lons'] = [row[9], row[11]]
        if with_shapes:
            try:
                trip_I = row[0]
                stop_seq1 = row[2]
                stop_seq2 = row[3]
                shaped = shapes.get_shape_between_stops(cur2,
                                                        trip_I,
                                                        stop_seq1,
                                                        stop_seq2)
                sf['lats'] = [row[8]]+shaped['lat']+[row[10]]
                sf['lons'] = [row[9]]+shaped['lon']+[row[11]]
                assert len(sf['lats']) == len(sf['lons'])
            except Exception as e:
                if e.message == "no such column: shape_break":
                    return False # return the new value of with_shapes
                else:
                    raise e
            return True
        else:
            return False # return the new value of with_shapes


    if return_dict:
        all_pair_freqs = { }
        for row in cur:
            id_ = '%s-%s'%(row[4],row[5])
            stop_freqs = all_pair_freqs[id_] = { }
            stop_freqs['name'] = '%s-%s'%(row[6],row[7])
            stop_freqs['freq'] = row[1]
            with_shapes = _lats_and_lons(row, stop_freqs, with_shapes)
        return all_pair_freqs

    all_pair_freqs = []
    for row in cur:
        id_ = '%s-%s'%(row[6],row[7])
        stop_freqs = { }
        stop_freqs['name'] = id_
        stop_freqs['freq'] = row[1]
        with_shapes = _lats_and_lons(row, stop_freqs, with_shapes)
        all_pair_freqs.append(stop_freqs)

    return dict(all_pair_freqs=all_pair_freqs)

def diff_pair_freqs(season1, day1, hour1, season2, day2, hour2, with_shapes=False):
    """Return change from time1 to time2, of pairs.

    This is time2-time1, or the amount added to time1 to get time2.
    """
    data1 = get_pair_freqs(season1, day1, hour1, return_dict=True, with_shapes=with_shapes)
    data2 = get_pair_freqs(season2, day2, hour2, return_dict=True, with_shapes=with_shapes)
    data2 = diff_create(data1, data2)
    # Transform back to list
    all_pair_freqs = [row for row in data2.itervalues() ]
    return dict(all_pair_freqs=all_pair_freqs)




if __name__ == "__main__":
    # print diff_pair_freqs('s2015', 'm', 8, 'w2015', 'm', 8, with_shapes=True)
    pass
