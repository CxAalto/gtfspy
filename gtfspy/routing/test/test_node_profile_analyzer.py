import unittest
from unittest import TestCase

from matplotlib import pyplot as plt

from gtfspy.routing.label import Label
from gtfspy.routing.node_profile_naive import NodeProfileNaive
from gtfspy.routing.node_profile_analyzer import NodeProfileAnalyzer


class TestNodeProfileAnalyzer(TestCase):

    def test_trip_duration_statistics_empty_profile(self):
        profile = NodeProfileNaive()
        analyzer = NodeProfileAnalyzer(profile, 0, 10)
        self.assertEqual(float('inf'), analyzer.max_trip_duration())
        self.assertEqual(float('inf'), analyzer.min_trip_duration())
        self.assertEqual(float('inf'), analyzer.mean_trip_duration())
        self.assertEqual(float('inf'), analyzer.median_trip_duration())

        self.assertEqual(float('inf'), analyzer.max_temporal_distance())
        self.assertEqual(float('inf'), analyzer.min_temporal_distance())
        self.assertEqual(float('inf'), analyzer.mean_temporal_distance())
        self.assertEqual(float('inf'), analyzer.median_temporal_distance())

    def test_trip_duration_statistics_simple(self):
        pairs = [
            Label(departure_time=1, arrival_time_target=2),
            Label(departure_time=2, arrival_time_target=4),
            Label(departure_time=4, arrival_time_target=5)
        ]
        profile = NodeProfileNaive()
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)
        analyzer = NodeProfileAnalyzer(profile, 0, 100)
        self.assertAlmostEqual(2, analyzer.max_trip_duration())
        self.assertAlmostEqual(1, analyzer.min_trip_duration())
        self.assertAlmostEqual(4 / 3.0, analyzer.mean_trip_duration())
        self.assertAlmostEqual(1, analyzer.median_trip_duration())

    def test_temporal_distance_statistics(self):
        pairs = [
            Label(departure_time=1, arrival_time_target=2),
            Label(departure_time=2, arrival_time_target=4),
            Label(departure_time=4, arrival_time_target=5)
        ]
        profile = NodeProfileNaive()
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)

        analyzer = NodeProfileAnalyzer(profile, 0, 3)
        self.assertAlmostEqual(4 - 1, analyzer.max_temporal_distance())  # 1 -wait-> 2 -travel->4
        self.assertAlmostEqual(1, analyzer.min_temporal_distance())
        self.assertAlmostEqual((1.5 * 1 + 2.5 * 1 + 2.5 * 1) / 3., analyzer.mean_temporal_distance())
        self.assertAlmostEqual(2.25, analyzer.median_temporal_distance())

    def test_temporal_distance_statistics_with_walk(self):
        pt1 = Label(departure_time=1, arrival_time_target=2)
        pt2 = Label(departure_time=4, arrival_time_target=5)   # not taken into account by the analyzer
        profile = NodeProfileNaive(1.5)
        assert isinstance(pt1, Label), type(pt1)
        profile.update_pareto_optimal_tuples(pt1)
        profile.update_pareto_optimal_tuples(pt2)
        analyzer = NodeProfileAnalyzer(profile, 0, 3)
        self.assertAlmostEqual(1.5, analyzer.max_temporal_distance())  # 1 -wait-> 2 -travel->4
        self.assertAlmostEqual(1, analyzer.min_temporal_distance())
        self.assertAlmostEqual((2.5 * 1.5 + 0.5 * 1.25) / 3., analyzer.mean_temporal_distance())
        self.assertAlmostEqual(1.5, analyzer.median_temporal_distance())

    @unittest.skip("Skipping plotting test")
    def test_all_plots(self):
        profile = NodeProfileNaive()
        profile.update_pareto_optimal_tuples(Label(departure_time=2 * 60, arrival_time_target=11 * 60))
        profile.update_pareto_optimal_tuples(Label(departure_time=20 * 60, arrival_time_target=25 * 60))
        profile.update_pareto_optimal_tuples(Label(departure_time=40 * 60, arrival_time_target=45 * 60))
        analyzer = NodeProfileAnalyzer(profile, 0, 60 * 60)
        analyzer.plot_temporal_distance_variation()
        analyzer.plot_temporal_distance_cdf()
        # analyzer.plot_temporal_distance_pdf()

        profile = NodeProfileNaive()
        profile.update_pareto_optimal_tuples(Label(departure_time=2 * 60, arrival_time_target=3 * 60))
        profile.update_pareto_optimal_tuples(Label(departure_time=4 * 60, arrival_time_target=25 * 60))
        analyzer = NodeProfileAnalyzer(profile, 0, 5 * 60)
        analyzer.plot_temporal_distance_variation()
        analyzer.plot_temporal_distance_cdf()
        # analyzer.plot_temporal_distance_pdf()

        pt1 = Label(departure_time=1, arrival_time_target=2)
        pt2 = Label(departure_time=4, arrival_time_target=5)   # not taken into account by the analyzer
        profile = NodeProfileNaive(1.5)
        profile.update_pareto_optimal_tuples(pt1)
        profile.update_pareto_optimal_tuples(pt2)
        analyzer = NodeProfileAnalyzer(profile, 0, 3)
        # analyzer.plot_temporal_distance_variation()
        analyzer.plot_temporal_distance_cdf()

        plt.show()

