import os
import unittest

from ..spreading.event import Event
from ..spreading.spreader import Spreader
from ..spreading.spreading_stop import SpreadingStop

from ..gtfs import GTFS


class SpreadingTest(unittest.TestCase):

    @staticmethod
    def test_get_min_visit_time():
        stop_I = 1
        min_transfer_time = 60
        ss = SpreadingStop(stop_I, min_transfer_time)
        assert ss.get_min_visit_time() == float('inf')
        ss.visit_events = [Event(10, 0, stop_I, stop_I, -1)]
        assert ss.get_min_visit_time() == 10
        ss.visit_events.append(Event(5, 0, stop_I, stop_I, -1))
        assert ss.get_min_visit_time() == 5

    @staticmethod
    def test_visit():
        stop_I = 1
        min_transfer_time = 60
        ss = SpreadingStop(stop_I, min_transfer_time)
        e = Event(10, 2, 2, stop_I, 1092)
        assert ss.visit(e)
        assert len(ss.visit_events) == 1
        assert ss.visit_events[0].from_stop_I == 2
        assert ss.visit_events[0].trip_I == 1092
        e2 = Event(66, 1, 2, 1092, stop_I)
        assert ss.visit(e2)
        e3 = Event(5, 1, 2, 1092, stop_I)
        assert ss.visit(e3)
        assert ss.get_min_visit_time() == 5
        e4 = Event(66, 1, 2, 1092, stop_I)
        assert not ss.visit(e4)
        e5 = Event(64, 1, 2, 1092, stop_I)
        assert ss.visit(e5)

    @staticmethod
    def test_can_infect():
        stop_I = 1
        min_transfer_time = 60
        ss = SpreadingStop(stop_I, min_transfer_time)
        trip_I = 2
        e = Event(0, -2, stop_I, 2, trip_I)
        assert not ss.can_infect(e)
        e2 = Event(5, 3, trip_I, 2, trip_I)
        assert not ss.can_infect(e2)
        ss.visit(e2)
        e3 = Event(14, 6, stop_I, stop_I + 1, trip_I)
        assert ss.can_infect(e3)
        e4 = Event(14, 6, stop_I, stop_I + 1, trip_I + 1)
        assert not ss.can_infect(e4)

    @staticmethod
    def test_get_trips():
        gtfs_source_dir = os.path.join(os.path.dirname(__file__), "test_data")
        gtfs = GTFS.from_directory_as_inmemory_db(gtfs_source_dir)
        first_day_start_ut, last_day_start_ut = gtfs.get_day_start_ut_span()
        stop_1_info = gtfs.get_stop_info(1)
        lat = stop_1_info.get("lat")
        lon = stop_1_info.get("lon")
        spreader = Spreader(gtfs, first_day_start_ut + 8 * 3600, lat, lon, 24 * 3600, 30, True, 0.5)
        trips = spreader.spread()
        assert isinstance(trips, dict)
        assert "trips" in trips
        assert len(trips["trips"]) > 0
        el = trips["trips"][0]
        keys = "lats lons times route_type name".split()
        for key in keys:
            assert key in el, el


