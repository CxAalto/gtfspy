from unittest import TestCase

from gtfspy.routing.analyses.node_profile_analyzer import NodeProfileAnalyzer
from gtfspy.routing.models import ParetoTuple
from gtfspy.routing.node_profile import NodeProfile


class TestNodeProfileAnalyzer(TestCase):

    def test_trip_duration_statistics_empty_profile(self):
        profile = NodeProfile()
        analyzer = NodeProfileAnalyzer(profile, 0, 10)
        self.assertEqual(None, analyzer.max_trip_duration())
        self.assertEqual(None, analyzer.min_trip_duration())
        self.assertEqual(None, analyzer.mean_trip_duration())
        self.assertEqual(None, analyzer.median_trip_duration())

    def test_trip_duration_statistics_simple(self):
        pairs = [
            ParetoTuple(departure_time=1, arrival_time_target=2),
            ParetoTuple(departure_time=2, arrival_time_target=4),
            ParetoTuple(departure_time=4, arrival_time_target=5)
        ]
        profile = NodeProfile()
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)
        analyzer = NodeProfileAnalyzer(profile, 0, 100)
        self.assertAlmostEqual(2, analyzer.max_trip_duration())
        self.assertAlmostEqual(1, analyzer.min_trip_duration())
        self.assertAlmostEqual(4 / 3.0, analyzer.mean_trip_duration())
        self.assertAlmostEqual(1, analyzer.median_trip_duration())

    def test_temporal_distance_statistics(self):
        pairs = [
            ParetoTuple(departure_time=1, arrival_time_target=2),
            ParetoTuple(departure_time=2, arrival_time_target=4),
            ParetoTuple(departure_time=4, arrival_time_target=5)
        ]
        profile = NodeProfile()
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)

        analyzer = NodeProfileAnalyzer(profile, 0, 3)
        self.assertAlmostEqual(4 - 1, analyzer.max_temporal_distance())  # 1 -wait-> 2 -travel->4
        self.assertAlmostEqual(1, analyzer.min_temporal_distance())
        self.assertAlmostEqual((1.5 * 1 + 2.5 * 1 + 2.5 * 1) / 3., analyzer.mean_temporal_distance())
        self.assertAlmostEqual(2.25, analyzer.median_temporal_distance())

