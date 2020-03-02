from pprint import pformat


class Connection:

    WALK_SEQ = -1
    WALK_TRIP_ID = -1

    def __init__(
        self,
        departure_stop,
        arrival_stop,
        departure_time,
        arrival_time,
        trip_id,
        seq,
        is_walk=False,
        arrival_stop_next_departure_time=float("inf"),
    ):
        self.departure_stop = departure_stop
        self.arrival_stop = arrival_stop
        self.departure_time = departure_time
        self.arrival_time = arrival_time
        self.trip_id = trip_id
        self.is_walk = is_walk
        self.seq = int(seq)
        self.arrival_stop_next_departure_time = arrival_stop_next_departure_time

    def duration(self):
        return self.arrival_time - self.departure_time

    def __str__(self):
        return pformat(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<%s:%s:%s:%s:%s:%s:%s:%s>" % (
            self.__class__.__name__,
            self.departure_stop,
            self.arrival_stop,
            self.departure_time,
            self.arrival_time,
            self.trip_id,
            self.is_walk,
            self.arrival_stop_next_departure_time,
        )

    def __hash__(self):
        return hash(self.__repr__())
