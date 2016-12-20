from collections import namedtuple

import numpy
from collections import defaultdict

_profile_block = namedtuple('ProfileBlock',
                            ['start_time',
                             'end_time',
                             'distance_start',
                             'distance_end'])


class ProfileBlock(_profile_block):

    def area(self):
        return self.width() * self.mean()

    def mean(self):
        return 0.5 * (self.distance_start + self.distance_end)

    def width(self):
        return self.end_time - self.start_time

    def max(self):
        return max(self.distance_start, self.distance_end)

    def min(self):
        return min(self.distance_start, self.distance_end)


class ProfileBlockAnalyzer:

    def __init__(self, profile_blocks, cutoff_distance):
        """
        Parameters
        ----------
        profile_blocks: list[ProfileBlock]
        """
        for i, block in enumerate(profile_blocks[:-1]):
            assert block.start_time <= block.end_time
            assert block.end_time == profile_blocks[i + 1].start_time
            assert block.distance_start >= block.distance_end

        self._profile_blocks = profile_blocks
        self._cutoff_distance = cutoff_distance  # e.g. walking distance
        self._start_time = profile_blocks[0].start_time
        self._end_time = profile_blocks[-1].end_time

    def mean(self):
        total_width = self._profile_blocks[-1].end_time - self._profile_blocks[0].start_time
        total_area = sum([block.area() for block in self._profile_blocks])
        return total_area / total_width

    def median(self):
        try:
            distance_split_points_ordered, norm_cdf = self._temporal_distance_cdf()
        except RuntimeError as e:
            return float('inf')

        if len(distance_split_points_ordered) == 0:
            return float('inf')

        left = numpy.searchsorted(norm_cdf, 0.5, side="left")
        right = numpy.searchsorted(norm_cdf, 0.5, side="right")
        if left == len(norm_cdf):
            return float('inf')
        elif left == right:
            left_cdf_val = norm_cdf[right - 1]
            right_cdf_val = norm_cdf[right]
            delta_y = right_cdf_val - left_cdf_val
            assert (delta_y > 0)
            delta_x = (distance_split_points_ordered[right] - distance_split_points_ordered[right - 1])
            median = (0.5 - left_cdf_val) / delta_y * delta_x + distance_split_points_ordered[right - 1]
            return median
        else:
            return distance_split_points_ordered[left]

    def min(self):
        return min([min(block.distance_end, block.distance_start) for block in self._profile_blocks])

    def max(self):
        return max([max(block.distance_end, block.distance_start) for block in self._profile_blocks])

    def largest_finite_distance(self):
        """
        Compute the maximum temporal distance.

        Returns
        -------
        max_temporal_distance : float
        """
        block_start_distances = [block.distance_start for block in self._profile_blocks if
                                 block.distance_start < float('inf')]
        block_end_distances = [block.distance_end for block in self._profile_blocks if
                               block.distance_end < float('inf')]
        distances = block_start_distances + block_end_distances
        if len(distances) > 0:
            return max(distances)
        else:
            return None

    def _temporal_distance_cdf(self):
        """
        Temporal distance cumulative density function.

        Returns
        -------
        x_values: numpy.array
            values for the x-axis
        cdf: numpy.array
            cdf values
        """
        distance_split_points = set()
        for block in self._profile_blocks:
            if block.distance_start != float('inf'):
                distance_split_points.add(block.distance_end)
                distance_split_points.add(block.distance_start)

        distance_split_points_ordered = numpy.array(sorted(list(distance_split_points)))
        temporal_distance_split_widths = distance_split_points_ordered[1:] - distance_split_points_ordered[:-1]
        trip_counts = numpy.zeros(len(temporal_distance_split_widths))
        delta_peaks = defaultdict(lambda: 0)

        for block in self._profile_blocks:
            if block.distance_start == block.distance_end:
                delta_peaks[block.distance_end] += block.width()
            else:
                start_index = numpy.searchsorted(distance_split_points_ordered, block.distance_end)
                end_index = numpy.searchsorted(distance_split_points_ordered, block.distance_start)
                trip_counts[start_index:end_index] += 1

        unnormalized_cdf = numpy.array([0] + list(numpy.cumsum(temporal_distance_split_widths * trip_counts)))
        if not (numpy.isclose(
                [unnormalized_cdf[-1]],
                [self._end_time - self._start_time - sum(delta_peaks.values())], atol=1E-4
                ).all()):
            print(unnormalized_cdf[-1], self._end_time - self._start_time - sum(delta_peaks.values()))
            raise RuntimeError("Something went wrong with cdf computation!")

        if len(delta_peaks) > 0:
            for peak in delta_peaks.keys():
                if peak == float('inf'):
                    continue
                index = numpy.nonzero(distance_split_points_ordered == peak)[0][0]
                unnormalized_cdf = numpy.insert(unnormalized_cdf, index, unnormalized_cdf[index])
                distance_split_points_ordered = numpy.insert(distance_split_points_ordered, index,
                                                             distance_split_points_ordered[index])
                # walk_waiting_time_fraction = walk_total_time / (self.end_time_dep - self.start_time_dep)
                unnormalized_cdf[(index + 1):] = unnormalized_cdf[(index + 1):] + delta_peaks[peak]

        norm_cdf = unnormalized_cdf / (unnormalized_cdf[-1] + delta_peaks[float('inf')])
        return distance_split_points_ordered, norm_cdf

    def get_vlines_and_slopes_for_plotting(self):
        vertical_lines = []
        slopes = []

        for i, block in enumerate(self._profile_blocks):
            distance_end_minutes = block.distance_end
            distance_start_minutes = block.distance_start

            slope = dict(x=[block.start_time, block.end_time],
                         y=[distance_start_minutes, distance_end_minutes])
            slopes.append(slope)

            if i != 0:
                # no vertical line for the first observation
                previous_duration_minutes = self._profile_blocks[i - 1].distance_end
                vertical_lines.append(dict(x=[block.start_time, block.start_time],
                                           y=[previous_duration_minutes, distance_start_minutes]))
        return vertical_lines, slopes

