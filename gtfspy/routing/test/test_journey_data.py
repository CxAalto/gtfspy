from unittest import TestCase

import networkx
from six import StringIO

from gtfspy.routing.connection import Connection
from gtfspy.routing.label import min_arrival_time_target, LabelTimeWithBoardingsCount, LabelTime
from gtfspy.routing.journey_data import JourneyDataManager
from gtfspy.routing.multi_objective_pseudo_connection_scan_profiler import MultiObjectivePseudoCSAProfiler
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective

import pyximport
pyximport.install()


class TestJourneyData(TestCase):
    # noinspection PyAttributeOutsideInit

    def setUp(self):
        event_list_raw_data = [
            (1, 2, 0, 10, "trip_1", 1),
            (2, 3, 10, 20, "trip_2", 1),
            (4, 5, 30, 40, "trip_3", 1),

        ]
        transit_connections = list(map(lambda el: Connection(*el), event_list_raw_data))
        walk_network = networkx.Graph()
        walk_network.add_edge(2, 4, {"d_walk": 10})
        walk_network.add_edge(3, 4, {"d_walk": 10})
        walk_network.add_edge(5, 6, {"d_walk": 10})
        walk_speed = 1
        target_stop = 5
        transfer_margin = 0
        start_time = 0
        end_time = 50

        csa_profile = MultiObjectivePseudoCSAProfiler(transit_connections, target_stop,
                                                      start_time, end_time, transfer_margin,
                                                      walk_network, walk_speed, track_vehicle_legs=False,
                                                      track_time=True, track_route=True)
        csa_profile.run()

        self.profiles = dict(csa_profile.stop_profiles)
        # self.jdm = JourneyDataManager()

    def test_import_with_route_to_db(self):
        pass