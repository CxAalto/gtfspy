from unittest import TestCase

import pyximport

from gtfspy.routing.journey_data import JourneyDataManager
from gtfspy.routing.label import LabelTimeWithBoardingsCount

pyximport.install()
import shutil
import os
from gtfspy.import_gtfs import import_gtfs
from gtfspy.routing.travel_impedance_data_store import TravelImpedanceDataStore


class TestJourneyData(TestCase):
    # noinspection PyAttributeOutsideInit

    def _import_sample_gtfs_db(self):
        import_gtfs([os.path.join(os.path.dirname(__file__), "../../test/test_data/test_gtfs.zip")], self.gtfs_path)

    def _remove_routing_test_data_directory_if_exists(self):
        try:
            shutil.rmtree(self.routing_tmp_test_data_dir)
        except FileNotFoundError:
            pass

    def _create_routing_test_data_directory(self):
        if not os.path.exists(self.routing_tmp_test_data_dir):
            os.makedirs(self.routing_tmp_test_data_dir)

    def setUp(self):
        self.routing_tmp_test_data_dir = "./tmp_routing_test_data/"
        self.gtfs_path = os.path.join(self.routing_tmp_test_data_dir, "test_gtfs.sqlite")
        self.data_store_path = os.path.join(self.routing_tmp_test_data_dir, "test_data_store.sqlite")
        self._remove_routing_test_data_directory_if_exists()
        self._create_routing_test_data_directory()

        self._import_sample_gtfs_db()
        self.jdm = JourneyDataManager(self.gtfs_path,
                                      os.path.join(self.routing_tmp_test_data_dir, "test_journeys.sqlite"),
                                      routing_params={"track_vehicle_legs": True})

    def tearDown(self):
        self._remove_routing_test_data_directory_if_exists()

    def test_boardings_computations_based_on_journeys(self):
        # input some journeys
        destination_stop = 1
        origin_stop = 2
        self.jdm.import_journey_data_for_target_stop(destination_stop,
                                                     {origin_stop:
                                                        [LabelTimeWithBoardingsCount(1, 2, 1, True),
                                                         LabelTimeWithBoardingsCount(2, 3, 2, True)]},
                                                     enforce_synchronous_writes=True
                                                     )
        self.jdm.compute_and_store_travel_impedance_measures(0, 2, self.data_store_path)
        store = TravelImpedanceDataStore(self.data_store_path)
        df = store.read_data_as_dataframe("temporal_distance")
        self.assertAlmostEqual(df.iloc[0]["min"], 1)
        self.assertAlmostEqual(df.iloc[0]["mean"], 1.5)
        self.assertAlmostEqual(df.iloc[0]["max"], 2.0)
        self.assertIn(df.iloc[0]["median"],[1, 2, 1.0, 1.5, 2.0])