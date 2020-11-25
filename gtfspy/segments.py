class Segments(object):
    def __init__(self, gtfs):
        self._gtfs

    def get_segments(self):
        """
        Get segment

        Returns
        -------
        segments: list[Segment]
        """
        cur = self._gtfs.get_cursor()

        # Find our IDs that are relevant.
        cur.execute(
            """SELECT trip_I, cnt, seq1, seq2, S1.code, S2.code,
                              S1.name AS name1,
                              S2.name AS name2,
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
                   """
        )


class Segment(object):
    def __init__(
        self, from_node, to_node, distance, time, vehicle_count, capacity_per_hour, lines, modes
    ):
        self.from_node = from_node
        self.to_node = to_node
        self.distance = distance
        self.time = time
        self.vehicle_count = vehicle_count
        self.capacity_per_hour = capacity_per_hour
        self.lines = lines
        self.modes = modes
