from collections import namedtuple

Event = namedtuple("Event", ["arr_time_ut", "dep_time_ut", "from_stop_I", "to_stop_I", "trip_I"])
