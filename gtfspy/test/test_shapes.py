from __future__ import print_function

import unittest

import numpy as np

from gtfspy.gtfs import GTFS
from gtfspy import shapes



class ShapesTest(unittest.TestCase):

    def test_shape_break_order(self):
        for trip_I in [
                # These trip IDs require the hsl-2015-07-12 DB.
                73775,  # Route 18 Eira -> Munkkivuori
                172258, # Route 94A in Helsinki, direction 0.
                84380,  # 36 in Helsinki.  Has lots of dead ends.
                83734,  # route 1032
                84044,  # route 1034
                240709, # 143K
                194350, # 802
                194530, # 802K
                270813, # P20
                270849, # P21
                ]:
            yield self.test_shape_break_order_1, trip_I
            pass
        #yield test_shape_break_order_1, 83734

    @unittest.skip("skipping test_shape_break_order_1")
    def test_shape_break_order_1(self, trip_I=73775):
        """This is to a bug related to shape alignment."""
        conn = GTFS('../scratch/db/hsl-2015-07-12.sqlite').conn
        cur = conn.cursor()

        cur.execute('''SELECT seq, lat, lon
                       FROM stop_times LEFT JOIN stops USING (stop_I)
                       WHERE trip_I=?
                       ORDER BY seq''',
                    (trip_I,))
        #print '%20s, %s'%(run_code, datetime.fromtimestamp(run_sch_starttime))
        stop_points = [ dict(seq=row[0],
                             lat=row[1],
                             lon=row[2])
                        for row in cur]

        # Get the shape points
        shape_id = cur.execute('''SELECT shape_id
                                  FROM trips WHERE trip_I=?''', (trip_I,)).fetchone()[0]
        shape_points = shapes.get_shape_points(cur, shape_id)
        breakpoints, badness \
              = shapes.find_segments(stop_points, shape_points)
        print(badness)
        if badness > 30:
            print("bad shape fit: %s (%s, %s)" % (badness, trip_I, shape_id))


        for b1, b2 in zip(breakpoints, sorted(breakpoints)):
            self.assertEqual(b1, b2)

    @staticmethod
    def test_interpolate_shape_times():
        shape_distances = [0, 2, 5, 10, 20, 100]
        shape_breaks = [0, 2, 5]
        stop_times = [0, 1, 20]
        result_should_be = [0, 0.4, 1, 1 + (19 * 5 / 95.), 1 + (19 * 15 / 95.), 20]

        result = shapes.interpolate_shape_times(shape_distances, shape_breaks, stop_times)
        assert len(result) == len(result_should_be)
        np.testing.assert_array_equal(result, result_should_be)

        shape_distances = [0, 1, 10]
        shape_breaks = [0, 1, 2]
        stop_times = [0, 10, 18]
        result_should_be = [0, 10, 18]
        result = shapes.interpolate_shape_times(shape_distances, shape_breaks, stop_times)
        assert len(result) == len(result_should_be)
        np.testing.assert_array_equal(result, result_should_be)
