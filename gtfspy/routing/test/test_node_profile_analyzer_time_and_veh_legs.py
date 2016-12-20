import unittest
from math import isnan
from unittest import TestCase

from gtfspy.routing.label import LabelTimeWithBoardingsCount
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.node_profile_analyzer_time_and_veh_legs import NodeProfileAnalyzerTimeAndVehLegs


class TestNodeProfileAnalyzerTimeAndVehLegs(TestCase):

    def setUp(self):
        self.label_class = LabelTimeWithBoardingsCount

    def _get_analyzer(self, labels, start_time, end_time, walk_to_target_duration=float('inf')):
        dep_times = list(set(map(lambda el: el.departure_time, labels)))
        p = NodeProfileMultiObjective(dep_times=dep_times,
                                      walk_to_target_duration=walk_to_target_duration,
                                      label_class=LabelTimeWithBoardingsCount)
        for label in labels:
            p.update([label])
        p.finalize()
        analyzer = NodeProfileAnalyzerTimeAndVehLegs(p, start_time, end_time)
        return analyzer

    def test_trip_duration_statistics_empty_profile(self):
        analyzer = self._get_analyzer([], 0, 10)

        self.assertTrue(isnan(analyzer.max_trip_n_boardings()))
        self.assertTrue(isnan(analyzer.min_trip_n_boardings()))
        self.assertTrue(isnan(analyzer.mean_trip_n_boardings()))
        self.assertTrue(isnan(analyzer.median_trip_n_boardings()))

    def test_temporal_distances_by_n_vehicles(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=12, n_boardings=4, first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=15, n_boardings=2, first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=17, n_boardings=1, first_leg_is_walk=False)
        ]
        analyzer = self._get_analyzer(labels, 0, 10, walk_to_target_duration=10)
        mean_temporal_distances = analyzer.median_temporal_distances()
        self.assertEqual(len(mean_temporal_distances), 4 + 1)
        for i in range(len(mean_temporal_distances) - 1):
            assert(mean_temporal_distances[i] >= mean_temporal_distances[i + 1])

    def test_n_boardings_on_shortest_paths(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=12, n_boardings=4,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=5, arrival_time_target=10, n_boardings=2,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=5, arrival_time_target=12, n_boardings=0,
                                        first_leg_is_walk=False)
        ]
        analyzer = self._get_analyzer(labels, 0, 10, walk_to_target_duration=10)
        self.assertEqual(analyzer.mean_n_boardings_along_shortest_paths(), 3)
        self.assertEqual(analyzer.min_n_boardings_along_shortest_paths(), 2)
        self.assertEqual(analyzer.max_n_boardings_along_shortest_paths(), 4)

    @unittest.skip
    def test_plot(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=20, arrival_time_target=22, n_boardings=5, first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=15, arrival_time_target=20, n_boardings=6, first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=14, arrival_time_target=21, n_boardings=5, first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=13, arrival_time_target=22, n_boardings=4, first_leg_is_walk=False),
            # LabelTimeWithBoardingsCount(departure_time=12, arrival_time_target=23, n_vehicle_legs=3),
            LabelTimeWithBoardingsCount(departure_time=11, arrival_time_target=24, n_boardings=2, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=25, n_boardings=1, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=5, arrival_time_target=10, n_boardings=1, first_leg_is_walk=True)
        ]
        analyzer = self._get_analyzer(labels, 0, 20, 35)
        fig = analyzer.plot_temporal_distance_variation()
        print(fig)
        import matplotlib.pyplot as plt
        plt.show()
