from unittest import TestCase

from gtfspy.routing.profile_block_analyzer import ProfileBlockAnalyzer
from gtfspy.routing.profile_block import ProfileBlock

class TestProfileBlockAnalyzer(TestCase):

    def test_interpolate(self):
        blocks = [ProfileBlock(0, 1, 2, 1), ProfileBlock(1, 2, 2, 2)]
        analyzer =  ProfileBlockAnalyzer(blocks, cutoff_distance=3.0)
        self.assertAlmostEqual(analyzer.interpolate(0.2), 1.8)
        self.assertAlmostEqual(analyzer.interpolate(1-10**-9), 1.)
        self.assertAlmostEqual(analyzer.interpolate(1), 1)
        self.assertAlmostEqual(analyzer.interpolate(1.+10**-9), 2)
        self.assertAlmostEqual(analyzer.interpolate(1.23), 2)
        self.assertAlmostEqual(analyzer.interpolate(2), 2)


