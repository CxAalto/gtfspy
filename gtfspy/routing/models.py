from collections import namedtuple

_pareto_named_tuple = namedtuple('ParetoTuple',
                         ['departure_time', 'arrival_time_target'])


class ParetoTuple(_pareto_named_tuple):

    def dominates(self, other):
        dominates = (
            (self.departure_time >= other.departure_time and self.arrival_time_target < other.arrival_time_target) or
            (self.departure_time > other.departure_time and self.arrival_time_target <= other.arrival_time_target)
        )
        return dominates

Connection = namedtuple('Connection',
                        ['departure_stop', 'arrival_stop', 'departure_time', 'arrival_time', 'trip_id'])
# add route + mode
