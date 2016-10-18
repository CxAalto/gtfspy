from collections import namedtuple

Connection = namedtuple('Connection',
                        ['departure_stop', 'arrival_stop', 'departure_time', 'arrival_time', 'trip_id'])


