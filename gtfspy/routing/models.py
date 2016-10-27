from collections import namedtuple

_connection = namedtuple('Connection',
                        ['departure_stop', 'arrival_stop', 'departure_time', 'arrival_time', 'trip_id', "is_walk"])


class Connection(_connection):
    def __new__(cls, departure_stop, arrival_stop, departure_time, arrival_time, trip_id, is_walk=False):
        return super(Connection, cls).__new__(cls,
                                              departure_stop,
                                              arrival_stop,
                                              departure_time,
                                              arrival_time,
                                              trip_id,
                                              is_walk)
