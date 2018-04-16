from unittest import TestCase

from gtfspy.routing.fastest_path_analyzer import FastestPathAnalyzer
from gtfspy.routing.label import LabelTimeWithBoardingsCount


class TestNodeJourneyPathAnalyzer(TestCase):

    def setUp(self):
        pass

    def _get_analyzer(self, labels, start_time_dep, end_time_dep, walk_duration=float('inf')):
        analyzer = FastestPathAnalyzer(labels, start_time_dep, end_time_dep, walk_duration=walk_duration)
        return analyzer

    def test_get_fastest_paths(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=8, arrival_time_target=22, n_boardings=1,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=24, n_boardings=1,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=12, arrival_time_target=26, n_boardings=1,
                                        first_leg_is_walk=False)
        ]
        analyzer = self._get_analyzer(labels, 0, 11, float('inf'))
        self.assertEqual(len(analyzer.get_fastest_path_labels(include_next_label_outside_interval=False)), 2)
        self.assertEqual(len(analyzer.get_fastest_path_labels(include_next_label_outside_interval=True)), 3)

        analyzer = self._get_analyzer(labels, 0, 10, float('inf'))

        self.assertEqual(len(analyzer.get_fastest_path_labels(include_next_label_outside_interval=False)), 2)
        self.assertEqual(len(analyzer.get_fastest_path_labels(include_next_label_outside_interval=True)), 2)

    def test_calculate_pre_journey_waiting_times_ignoring_direct_walk(self):
        # Needs LabelGeneric
        self.skipTest("test missing")

    def test_get_labels_faster_than_walk(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=8, arrival_time_target=10, n_boardings=1,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=12, n_boardings=1,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=12, arrival_time_target=15, n_boardings=1,
                                        first_leg_is_walk=False)
        ]
        analyzer = self._get_analyzer(labels, 0, 11, float('inf'))
        self.assertEqual(len(analyzer.get_labels_faster_than_walk()), 3)
        analyzer = self._get_analyzer(labels, 0, 11, 2)
        self.assertEqual(len(analyzer.get_labels_faster_than_walk()), 2)
        analyzer = self._get_analyzer(labels, 0, 11, 1)
        self.assertEqual(len(analyzer.get_labels_faster_than_walk()), 0)

    def test_calculate_pre_journey_waiting_times_to_list(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=8, arrival_time_target=10, n_boardings=1,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=10, arrival_time_target=24, n_boardings=1,
                                        first_leg_is_walk=False),
            LabelTimeWithBoardingsCount(departure_time=12, arrival_time_target=26, n_boardings=1,
                                        first_leg_is_walk=False)
        ]

        analyzer = self._get_analyzer(labels, 0, 11, float('inf'))
        pre_journey_waits, direct_walk_time = analyzer.calculate_pre_journey_waiting_times_to_list()
        self.assertEqual(pre_journey_waits, [8.0, 2.0, 1.0])
        self.assertEqual(direct_walk_time, 0.0)

        analyzer = self._get_analyzer(labels, 0, 11, 3)
        pre_journey_waits, direct_walk_time = analyzer.calculate_pre_journey_waiting_times_to_list()
        self.assertEqual(pre_journey_waits, None)
        self.assertEqual(direct_walk_time, None)

        analyzer = self._get_analyzer(labels, 0, 11, 1)
        pre_journey_waits, direct_walk_time = analyzer.calculate_pre_journey_waiting_times_to_list()
        self.assertEqual(pre_journey_waits, [])
        self.assertEqual(direct_walk_time, 11.0)

        analyzer = self._get_analyzer(labels, 0, 11.5, 15)
        pre_journey_waits, direct_walk_time = analyzer.calculate_pre_journey_waiting_times_to_list()
        self.assertEqual(pre_journey_waits, [8.0, 1.0, 0.5])
        self.assertEqual(direct_walk_time, 2)

        analyzer = self._get_analyzer(labels, 0, 10.5, 15)
        pre_journey_waits, direct_walk_time = analyzer.calculate_pre_journey_waiting_times_to_list()
        self.assertEqual(pre_journey_waits, [8.0, 1.0, 0.0])
        self.assertEqual(direct_walk_time, 1.5)

        analyzer = self._get_analyzer(labels, 0, 13, 15)
        pre_journey_waits, direct_walk_time = analyzer.calculate_pre_journey_waiting_times_to_list()
        self.assertEqual(pre_journey_waits, None)
        self.assertEqual(direct_walk_time, None)





