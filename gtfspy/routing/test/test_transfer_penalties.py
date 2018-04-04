from unittest import TestCase

from gtfspy.routing.fastest_path_analyzer import FastestPathAnalyzer
from gtfspy.routing.label import LabelTimeWithBoardingsCount
from gtfspy.routing.transfer_penalties import add_transfer_penalties_to_arrival_times, get_fastest_path_analyzer_after_transfer_penalties


class TestTransferPenalties(TestCase):

    def test_add_transfer_penalty_to_arrival_time(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=0, arrival_time_target=1, n_boardings=0, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=1, arrival_time_target=2, n_boardings=1, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=2, arrival_time_target=3, n_boardings=2, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=3, arrival_time_target=4, n_boardings=3, first_leg_is_walk=True)
        ]
        penalized_labels = add_transfer_penalties_to_arrival_times(labels, penalty_seconds=10, ignore_first_boarding=False)
        self.assertEqual(penalized_labels[0].arrival_time_target, 1)
        self.assertEqual(penalized_labels[1].arrival_time_target, 12)
        self.assertEqual(penalized_labels[2].arrival_time_target, 23)
        self.assertEqual(penalized_labels[3].arrival_time_target, 34)

        penalized_labels = add_transfer_penalties_to_arrival_times(labels, penalty_seconds=10, ignore_first_boarding=True)
        self.assertEqual(penalized_labels[0].arrival_time_target, 1)
        self.assertEqual(penalized_labels[1].arrival_time_target, 2)
        self.assertEqual(penalized_labels[2].arrival_time_target, 13)
        self.assertEqual(penalized_labels[3].arrival_time_target, 24)

    def test_get_time_analyzer_with_transfer_penalties(self):
        labels = [
            LabelTimeWithBoardingsCount(departure_time=0, arrival_time_target=1, n_boardings=0, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=1, arrival_time_target=2, n_boardings=1, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=2, arrival_time_target=3, n_boardings=2, first_leg_is_walk=True),
            LabelTimeWithBoardingsCount(departure_time=3, arrival_time_target=4, n_boardings=3, first_leg_is_walk=True)
        ]
        ta = FastestPathAnalyzer(labels, -1, 3, walk_duration=10).get_time_analyzer()
        ta.mean_temporal_distance()
        # For debugging:
        # ax = ta.plot_temporal_distance_profile()
        # ax.figure.savefig("/tmp/profile_normal.pdf")

        fpa_with_penalties = get_fastest_path_analyzer_after_transfer_penalties(labels, -1, 3, 60, walk_duration=600)
        ta_with_penalties = fpa_with_penalties.get_time_analyzer()
        # For debugging:
        # ax_penalties = ta_with_penalties.plot_temporal_distance_profile()
        # ax_penalties.figure.savefig("/tmp/profile_penalties.pdf")
        self.assertAlmostEqual(ta_with_penalties.mean_temporal_distance() - ta.mean_temporal_distance(), 60 * 3/4.)

