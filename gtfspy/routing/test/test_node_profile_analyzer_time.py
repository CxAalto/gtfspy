import unittest
from unittest import TestCase

import numpy
from matplotlib import pyplot as plt

from gtfspy.routing.label import LabelTimeSimple
from gtfspy.routing.node_profile_simple import NodeProfileSimple
from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime


class TestNodeProfileAnalyzerTime(TestCase):

    def test_trip_duration_statistics_empty_profile(self):
        profile = NodeProfileSimple()
        analyzer = NodeProfileAnalyzerTime(profile, 0, 10)
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
            LabelTimeSimple(departure_time=1, arrival_time_target=2),
            LabelTimeSimple(departure_time=2, arrival_time_target=4),
            LabelTimeSimple(departure_time=4, arrival_time_target=5)
        ]
        profile = NodeProfileSimple()
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 100)
        self.assertAlmostEqual(2, analyzer.max_trip_duration())
        self.assertAlmostEqual(1, analyzer.min_trip_duration())
        self.assertAlmostEqual(4 / 3.0, analyzer.mean_trip_duration())
        self.assertAlmostEqual(1, analyzer.median_trip_duration())

    def test_temporal_distance_statistics(self):
        pairs = [
            LabelTimeSimple(departure_time=1, arrival_time_target=2),
            LabelTimeSimple(departure_time=2, arrival_time_target=4),
            LabelTimeSimple(departure_time=4, arrival_time_target=5)
        ]
        profile = NodeProfileSimple()
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)

        analyzer = NodeProfileAnalyzerTime(profile, 0, 3)
        self.assertAlmostEqual(4 - 1, analyzer.max_temporal_distance())  # 1 -wait-> 2 -travel->4
        self.assertAlmostEqual(1, analyzer.min_temporal_distance())
        self.assertAlmostEqual((1.5 * 1 + 2.5 * 1 + 2.5 * 1) / 3., analyzer.mean_temporal_distance())
        self.assertAlmostEqual(2.25, analyzer.median_temporal_distance())

    def test_temporal_distances_no_transit_trips_within_range(self):
        pairs = [
            LabelTimeSimple(departure_time=11, arrival_time_target=12),
        ]
        profile = NodeProfileSimple(walk_to_target_duration=5)
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 10)
        self.assertAlmostEqual(5, analyzer.max_temporal_distance())
        self.assertAlmostEqual(2, analyzer.min_temporal_distance())
        self.assertAlmostEqual((7 * 5 + 3 * (5 + 2) / 2.) / 10.0, analyzer.mean_temporal_distance())
        self.assertAlmostEqual(5, analyzer.median_temporal_distance())

    def test_temporal_distances_no_transit_trips_within_range_and_no_walk(self):
        pairs = [
            LabelTimeSimple(departure_time=11, arrival_time_target=12),
        ]
        profile = NodeProfileSimple(walk_to_target_duration=float('inf'))
        for pair in pairs:
            profile.update_pareto_optimal_tuples(pair)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 10)
        self.assertAlmostEqual(12, analyzer.max_temporal_distance())
        self.assertAlmostEqual(2, analyzer.min_temporal_distance())
        self.assertAlmostEqual((12 + 2) / 2.0, analyzer.mean_temporal_distance())
        self.assertAlmostEqual((12 + 2) / 2.0, analyzer.median_temporal_distance())

    def test_time_offset(self):
        max_distances = []
        for offset in [0, 10, 100, 1000]:
            labels = [
                LabelTimeSimple(departure_time=7248 + offset, arrival_time_target=14160 + offset),
            ]
            profile = NodeProfileSimple(walk_to_target_duration=float('inf'))
            for label in labels:
                profile.update_pareto_optimal_tuples(label)
            analyzer = NodeProfileAnalyzerTime(profile, 0 + offset, 7200 + offset)
            max_distances.append(analyzer.max_temporal_distance())
        max_distances = numpy.array(max_distances)
        assert((max_distances == max_distances[0]).all())
        # self.assertAlmostEqual(12, analyzer.max_temporal_distance())
        # self.assertAlmostEqual(2, analyzer.min_temporal_distance())
        # self.assertAlmostEqual((12 + 2) / 2.0, analyzer.mean_temporal_distance())
        # self.assertAlmostEqual((12 + 2) / 2.0, analyzer.median_temporal_distance())

    def test_temporal_distance_statistics_with_walk(self):
        pt1 = LabelTimeSimple(departure_time=1, arrival_time_target=2)
        pt2 = LabelTimeSimple(departure_time=4, arrival_time_target=5)   # not taken into account by the analyzer
        profile = NodeProfileSimple(1.5)
        assert isinstance(pt1, LabelTimeSimple), type(pt1)
        profile.update_pareto_optimal_tuples(pt1)
        profile.update_pareto_optimal_tuples(pt2)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 3)
        self.assertAlmostEqual(1.5, analyzer.max_temporal_distance())  # 1 -wait-> 2 -travel->4
        self.assertAlmostEqual(1, analyzer.min_temporal_distance())
        self.assertAlmostEqual((2.5 * 1.5 + 0.5 * 1.25) / 3., analyzer.mean_temporal_distance())
        self.assertAlmostEqual(1.5, analyzer.median_temporal_distance())

    def test_temporal_distance_statistics_with_walk2(self):
        pt1 = LabelTimeSimple(departure_time=10, arrival_time_target=30)
        profile = NodeProfileSimple(25)
        profile.update_pareto_optimal_tuples(pt1)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 10)
        # analyzer.plot_temporal_distance_profile()
        # plt.show()

        self.assertAlmostEqual(25, analyzer.max_temporal_distance())  # 1 -wait-> 2 -travel->4
        self.assertAlmostEqual(20, analyzer.min_temporal_distance())
        self.assertAlmostEqual((7.5 * 25 + 2.5 * 20) / 10.0, analyzer.mean_temporal_distance())
        self.assertAlmostEqual(25, analyzer.median_temporal_distance())

    def test_temporal_distance_pdf_with_walk(self):
        profile = NodeProfileSimple(25)
        pt1 = LabelTimeSimple(departure_time=10, arrival_time_target=30)
        profile.update_pareto_optimal_tuples(pt1)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 10)

        self.assertEqual(len(analyzer.profile_block_analyzer._temporal_distance_pdf()), 3)

        split_points, densities, delta_peaks = analyzer.profile_block_analyzer._temporal_distance_pdf()
        self.assertEqual(len(split_points), 2)
        self.assertEqual(split_points[0], 20)
        self.assertEqual(split_points[1], 25)

        self.assertEqual(len(densities), 1)
        self.assertEqual(densities[0], 0.1)

        self.assertIn(25, delta_peaks)
        self.assertEqual(delta_peaks[25], 0.5)


    @unittest.skip("Skipping plotting test")
    def test_all_plots(self):
        profile = NodeProfileSimple(25)
        pt1 = LabelTimeSimple(departure_time=10, arrival_time_target=30)
        profile.update_pareto_optimal_tuples(pt1)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 10)
        analyzer.plot_temporal_distance_profile(plot_tdist_stats=True)
        analyzer.plot_temporal_distance_cdf()
        analyzer.plot_temporal_distance_pdf()
        plt.show()

        profile = NodeProfileSimple()
        profile.update_pareto_optimal_tuples(LabelTimeSimple(departure_time=2 * 60, arrival_time_target=11 * 60))
        profile.update_pareto_optimal_tuples(LabelTimeSimple(departure_time=20 * 60, arrival_time_target=25 * 60))
        profile.update_pareto_optimal_tuples(LabelTimeSimple(departure_time=40 * 60, arrival_time_target=45 * 60))
        analyzer = NodeProfileAnalyzerTime(profile, 0, 60 * 60)
        analyzer.plot_temporal_distance_profile()
        analyzer.plot_temporal_distance_cdf()
        analyzer.plot_temporal_distance_pdf()

        profile = NodeProfileSimple()
        profile.update_pareto_optimal_tuples(LabelTimeSimple(departure_time=2 * 60, arrival_time_target=3 * 60))
        profile.update_pareto_optimal_tuples(LabelTimeSimple(departure_time=4 * 60, arrival_time_target=25 * 60))
        analyzer = NodeProfileAnalyzerTime(profile, 0, 5 * 60)
        analyzer.plot_temporal_distance_profile()
        analyzer.plot_temporal_distance_cdf()
        analyzer.plot_temporal_distance_pdf()

        pt1 = LabelTimeSimple(departure_time=1, arrival_time_target=2)
        pt2 = LabelTimeSimple(departure_time=4, arrival_time_target=5)   # not taken into account by the analyzer
        profile = NodeProfileSimple(1.5)
        profile.update_pareto_optimal_tuples(pt1)
        profile.update_pareto_optimal_tuples(pt2)
        analyzer = NodeProfileAnalyzerTime(profile, 0, 3)
        analyzer.plot_temporal_distance_profile()
        analyzer.plot_temporal_distance_cdf()

        plt.show()

