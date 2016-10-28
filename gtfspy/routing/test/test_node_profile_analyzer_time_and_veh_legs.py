from unittest import TestCase

from gtfspy.routing.label import LabelTimeAndVehLegCount
from gtfspy.routing.node_profile_multiobjective import NodeProfileMultiObjective
from gtfspy.routing.node_profile_analyzer_time_and_veh_legs import NodeProfileAnalyzerTimeAndVehLegs


class TestNodeProfileAnalyzerTime(TestCase):

    def setUp(self):
        self.label_class = LabelTimeAndVehLegCount

    def test_trip_duration_statistics_empty_profile(self):
        profile = NodeProfileMultiObjective()
        analyzer = NodeProfileAnalyzerTimeAndVehLegs(profile, 0, 10)
        self.assertEqual(None, analyzer.max_trip_n_veh_legs())
        self.assertEqual(None, analyzer.min_trip_n_veh_legs())
        self.assertEqual(None, analyzer.mean_trip_n_veh_legs())
        self.assertEqual(None, analyzer.median_trip_n_veh_legs())

    def test_plot(self):
        p = NodeProfileMultiObjective(25, label_class=LabelTimeAndVehLegCount)
        p.update(LabelTimeAndVehLegCount(departure_time=20, arrival_time_target=22, n_vehicle_legs=5))
        p.update(LabelTimeAndVehLegCount(departure_time=15, arrival_time_target=20, n_vehicle_legs=6))
        p.update(LabelTimeAndVehLegCount(departure_time=14, arrival_time_target=21, n_vehicle_legs=5))
        p.update(LabelTimeAndVehLegCount(departure_time=13, arrival_time_target=22, n_vehicle_legs=4))
        p.update(LabelTimeAndVehLegCount(departure_time=12, arrival_time_target=23, n_vehicle_legs=3))
        p.update(LabelTimeAndVehLegCount(departure_time=11, arrival_time_target=24, n_vehicle_legs=2))
        p.update(LabelTimeAndVehLegCount(departure_time=10, arrival_time_target=25, n_vehicle_legs=1))
        p.update(LabelTimeAndVehLegCount(departure_time=5, arrival_time_target=10, n_vehicle_legs=1))
        analyzer = NodeProfileAnalyzerTimeAndVehLegs(p, 0, 25)
        analyzer.plot_temporal_distance_variation()
        import matplotlib.pyplot as plt
        plt.show()
