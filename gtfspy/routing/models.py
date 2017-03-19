from pprint import pformat


class Connection:

    def __init__(self, departure_stop, arrival_stop, departure_time, arrival_time, trip_id,
                 is_walk=False, arrival_stop_next_departure_time=float('inf')):
        self.departure_stop = departure_stop
        self.arrival_stop = arrival_stop
        self.departure_time = departure_time
        self.arrival_time = arrival_time
        self.trip_id = trip_id
        self.is_walk = is_walk
        self.arrival_stop_next_departure_time = arrival_stop_next_departure_time

        self.departure_coordinate = None
        self.arrival_coordinate = None
        self.route_name = None
        self.mode = None
        self.duration = None
        self.distance = None

    def get_coordinate_tuple(self):
        coordinate_tuple = (self.departure_coordinate, self.arrival_coordinate)
        return coordinate_tuple

    def duration(self):
        return self.arrival_time - self.departure_time

    def __str__(self):
        return pformat(self.__dict__)

