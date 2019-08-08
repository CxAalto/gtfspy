from unittest import TestCase

from gtfspy.routing.connection import Connection
from gtfspy.routing.journey_path_analyzer import NodeJourneyPathAnalyzer
from gtfspy.routing.label import LabelTimeBoardingsAndRoute


class TestNodeJourneyPathAnalyzer(TestCase):

    def setUp(self):
        self.label_class = LabelTimeBoardingsAndRoute

    @staticmethod
    def _get_analyzer(labels, start_time, end_time, walk_to_target_duration=float('inf')):
        analyzer = NodeJourneyPathAnalyzer(labels, walk_to_target_duration, start_time, end_time, origin_stop=791)
        return analyzer

    def _get_simple_label_list(self, increments=None):
        if not increments:
            increments = [100, 200, 300]
        return [self._get_incremented_label(x)[0] for x in increments]

    @staticmethod
    def _get_incremented_label(increment):
        connection_1c = Connection(1117, 1040, 420+increment, 714+increment, None, True, float("inf"))
        connection_1b = Connection(1090, 1117, 300+increment, 420+increment, 1, False, 600)
        connection_1a = Connection(791, 1090, 180+increment, 300+increment, 121017, False, 480)

        label_1c = LabelTimeBoardingsAndRoute(420.0+increment, 714.0+increment, 0, 294, False, connection_1c, None)
        label_1b = LabelTimeBoardingsAndRoute(300.0+increment, 714.0+increment, 1, 414, False, connection_1b, label_1c)
        label_1a = LabelTimeBoardingsAndRoute(180.0+increment, 714.0+increment, 1, 534, False, connection_1a, label_1b)

        return [label_1a]

    def test_unpackjourneys(self):
        """
        Cases to include,
        walking dominates always,
        walking partially dominates
        ,return,
        """
        label_list = []
        for i in [0, 600]:
            label_list += self._get_incremented_label(increment=i)

        njpa = self._get_analyzer(label_list, 0, 700, float("inf"))

        self.assertEqual(njpa.get_fp_all_journey_stops(), [[791, 1090, 1117, 1040], [791, 1090, 1117, 1040]])
        self.assertEqual(njpa.journey_set_variants, [(791, 1090)])

        njpa = self._get_analyzer(label_list, 0, 700, 600)
        self.assertEqual(njpa.get_fp_all_journey_stops(), [[791, 1090, 1117, 1040], [791, 1090, 1117, 1040]])
        self.assertEqual(set(njpa.journey_set_variants), {(791, 1090), (791,)})

        njpa = self._get_analyzer(label_list, 0, 700, 500)
        self.assertEqual(njpa.get_fp_all_journey_stops(), [])
        self.assertEqual(set(njpa.journey_set_variants), {(791,)})

    def test_basic_diversity(self):
        """
        This tests the measures that are based on njpa.journey_set_variants and njpa.variant_proportions
        :return:
        """
        label_list = self._get_simple_label_list()
        njpa = self._get_analyzer(label_list, 0, 1000, float("inf"))

        njpa.journey_set_variants = [(1, 2, 3), (1, 3, 4), (3, 4, 5)]
        njpa.variant_proportions = [1/3, 1/3, 1/3]

        self.assertEqual(njpa.most_probable_departure_stop(), 2/3)
        self.assertEqual(njpa.most_probable_journey_variant(), 1/3)
        self.assertEqual(njpa.number_of_fp_journey_variants(), 3)
        self.assertEqual(njpa.simpson_diversity(stop_sets=njpa.journey_set_variants), 1 / 3)
        self.assertEqual(njpa.simpson_diversity(weights=njpa.variant_proportions), 1 / 3)

        njpa.journey_set_variants = [frozenset({3, 4, 5})]
        njpa.variant_proportions = [1]

        self.assertEqual(njpa.most_probable_departure_stop(), 1)
        self.assertEqual(njpa.most_probable_journey_variant(), 1)
        self.assertEqual(njpa.number_of_fp_journey_variants(), 1)
        self.assertEqual(njpa.simpson_diversity(stop_sets=njpa.journey_set_variants), 1)
        self.assertEqual(njpa.simpson_diversity(weights=njpa.variant_proportions), 1)

        njpa.journey_set_variants = []
        njpa.variant_proportions = []

        self.assertEqual(njpa.most_probable_departure_stop(), None)
        self.assertEqual(njpa.most_probable_journey_variant(), None)
        self.assertEqual(njpa.number_of_fp_journey_variants(), None)
        self.assertEqual(njpa.simpson_diversity(stop_sets=njpa.journey_set_variants), None)
        self.assertEqual(njpa.simpson_diversity(weights=njpa.variant_proportions), None)

        # With walking distance
        njpa = self._get_analyzer(label_list, 0, 1000, 500)

        njpa.journey_set_variants = []
        njpa.variant_proportions = []
        njpa.labels_faster_than_walk = []

        self.assertEqual(njpa.most_probable_departure_stop(), None)
        self.assertEqual(njpa.most_probable_journey_variant(), None)
        self.assertEqual(njpa.number_of_fp_journey_variants(), None)
        self.assertEqual(njpa.simpson_diversity(stop_sets=njpa.journey_set_variants), None)
        self.assertEqual(njpa.simpson_diversity(weights=njpa.variant_proportions), None)

        njpa.journey_set_variants = [frozenset({1, 2, 3}), frozenset({1, 2, 3}), frozenset({1, 2, 3})]
        njpa.variant_proportions = [1]
        njpa.labels_faster_than_walk = 3 * self._get_simple_label_list()

        self.assertEqual(njpa.most_probable_departure_stop(), 1)
        self.assertEqual(njpa.most_probable_journey_variant(), 1)
        self.assertEqual(njpa.number_of_fp_journey_variants(), 3)
        self.assertEqual(njpa.simpson_diversity(stop_sets=njpa.journey_set_variants), 1)
        self.assertEqual(njpa.simpson_diversity(weights=njpa.variant_proportions), 1)

    def test_label_based_diversity_measures(self):
        """
        :return:
        """
        label_list = self._get_simple_label_list()
        print(label_list)
        # No journey after timespan
        njpa = self._get_analyzer(label_list, 0, 1000, float("inf"))

        self.assertEqual(njpa.number_of_fp_journeys(), 3)
        self.assertEqual(njpa.number_of_fp_journey_variants(), 1)
        self.assertEqual(njpa.number_of_journeys(), 3)
        self.assertEqual(njpa.number_of_journey_variants(), 1)
        self.assertEqual(njpa.number_of_most_common_journey_variant(), 3)
        self.assertEqual(njpa.proportion_fp_journeys(), 1)
        self.assertEqual(njpa.largest_headway_gap(), None)
        self.assertEqual(njpa.expected_pre_journey_waiting_time(), None)

        print(njpa.min_temporal_distance())
        print(njpa.max_temporal_distance())
        # walking is faster
        njpa = self._get_analyzer(label_list, 0, 1000, 500)
        print(njpa.min_temporal_distance())

        print(njpa.max_temporal_distance())
        self.assertEqual(njpa.number_of_fp_journeys(), float("inf"))
        self.assertEqual(njpa.number_of_fp_journey_variants(), 1)
        self.assertEqual(njpa.number_of_journeys(), float("inf"))
        self.assertEqual(njpa.number_of_journey_variants(), 1)
        self.assertEqual(njpa.number_of_most_common_journey_variant(), float("inf"))
        self.assertEqual(njpa.proportion_fp_journeys(), 1)
        self.assertEqual(njpa.largest_headway_gap(), None)
        self.assertEqual(njpa.expected_pre_journey_waiting_time(), None)
        # walking is faster
        njpa = self._get_analyzer(label_list, 0, 400, float("inf"))
        print(njpa.min_temporal_distance())

        print(njpa.max_temporal_distance())
        self.assertEqual(njpa.number_of_fp_journeys(), 3)
        self.assertEqual(njpa.number_of_fp_journey_variants(), 1)
        self.assertEqual(njpa.number_of_journeys(), 3)
        self.assertEqual(njpa.number_of_journey_variants(), 1)
        self.assertEqual(njpa.number_of_most_common_journey_variant(), 3)
        self.assertEqual(njpa.proportion_fp_journeys(), 1)
        self.assertEqual(njpa.largest_headway_gap(), 4.67)  # 280/60
        self.assertEqual(njpa.expected_pre_journey_waiting_time(), 1.85)  # 111/60

