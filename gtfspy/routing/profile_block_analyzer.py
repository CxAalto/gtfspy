from collections import defaultdict

import numpy

from gtfspy.routing.profile_block import ProfileBlock


class ProfileBlockAnalyzer:
    def __init__(self, profile_blocks, cutoff_distance=None, **kwargs):
        """
        Parameters
        ----------
        profile_blocks: list[gtfspy.routing.profile_block.ProfileBlock]
        """
        for i, block in enumerate(profile_blocks[:-1]):
            assert block.start_time < block.end_time
            assert block.end_time == profile_blocks[i + 1].start_time
            assert block.distance_start >= block.distance_end

        self._profile_blocks = profile_blocks
        self._start_time = profile_blocks[0].start_time
        self._end_time = profile_blocks[-1].end_time
        self._cutoff_distance = cutoff_distance
        if cutoff_distance is not None:
            self._apply_cutoff(cutoff_distance)

        self.from_stop_I = None
        self.to_stop_I = None

        for key, value in kwargs.items():
            if key == "from_stop_I":
                self.from_stop_I = value
            if key == "to_stop_I":
                self.to_stop_I = value

    def _apply_cutoff(self, cutoff_distance):
        for block in list(self._profile_blocks):
            block_max = max(block.distance_start, block.distance_end)
            if block_max > cutoff_distance:
                print("applying cutoff")
                blocks = []
                if block.distance_start == block.distance_end or (
                    block.distance_start > cutoff_distance and block.distance_end > cutoff_distance
                ):
                    blocks.append(
                        ProfileBlock(
                            distance_end=cutoff_distance,
                            distance_start=cutoff_distance,
                            start_time=block.start_time,
                            end_time=block.end_time,
                        )
                    )
                else:
                    if block.distance_end >= cutoff_distance:
                        assert block.distance_end < cutoff_distance
                    split_point_x = (
                        block.start_time
                        + (block.distance_start - cutoff_distance)
                        / (block.distance_start - block.distance_end)
                        * block.width()
                    )
                    if block.distance_start > block.distance_end:
                        start_distance = cutoff_distance
                        end_distance = block.distance_end
                    else:
                        start_distance = block.distance_start
                        end_distance = cutoff_distance
                    first_block = ProfileBlock(
                        block.start_time, split_point_x, start_distance, cutoff_distance
                    )
                    second_block = ProfileBlock(
                        split_point_x, block.end_time, cutoff_distance, end_distance
                    )
                    blocks.append(first_block)
                    blocks.append(second_block)
                index = self._profile_blocks.index(block)
                self._profile_blocks[index : index + 1] = blocks

    def mean(self):
        total_width = self._profile_blocks[-1].end_time - self._profile_blocks[0].start_time
        total_area = sum([block.area() for block in self._profile_blocks])
        return total_area / total_width

    def median(self):
        try:
            distance_split_points_ordered, norm_cdf = self._temporal_distance_cdf()
        except RuntimeError:
            return float("inf")

        if len(distance_split_points_ordered) == 0:
            return float("inf")

        left = numpy.searchsorted(norm_cdf, 0.5, side="left")
        right = numpy.searchsorted(norm_cdf, 0.5, side="right")
        if left == len(norm_cdf):
            return float("inf")
        elif left == right:
            left_cdf_val = norm_cdf[right - 1]
            right_cdf_val = norm_cdf[right]
            delta_y = right_cdf_val - left_cdf_val
            assert delta_y > 0
            delta_x = (
                distance_split_points_ordered[right] - distance_split_points_ordered[right - 1]
            )
            median = (0.5 - left_cdf_val) / delta_y * delta_x + distance_split_points_ordered[
                right - 1
            ]
            return median
        else:
            return distance_split_points_ordered[left]

    def min(self):  # noqa: A003
        return min(
            [min(block.distance_end, block.distance_start) for block in self._profile_blocks]
        )

    def max(self):  # noqa: A003
        return max(
            [max(block.distance_end, block.distance_start) for block in self._profile_blocks]
        )

    def largest_finite_distance(self):
        """
        Compute the maximum temporal distance.

        Returns
        -------
        max_temporal_distance : float
        """
        block_start_distances = [
            block.distance_start
            for block in self._profile_blocks
            if block.distance_start < float("inf")
        ]
        block_end_distances = [
            block.distance_end
            for block in self._profile_blocks
            if block.distance_end < float("inf")
        ]
        distances = block_start_distances + block_end_distances
        if len(distances) > 0:
            return max(distances)
        else:
            return None

    def summary_as_dict(self):
        summary = {
            "max": self.max(),
            "min": self.min(),
            "mean": self.mean(),
            "median": self.median(),
        }
        if hasattr(self, "from_stop_I"):
            summary["from_stop_I"] = self.from_stop_I
        if hasattr(self, "to_stop_I"):
            summary["to_stop_I"] = self.to_stop_I
        return summary

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
            if block.distance_start != float("inf"):
                distance_split_points.add(block.distance_end)
                distance_split_points.add(block.distance_start)

        distance_split_points_ordered = numpy.array(sorted(list(distance_split_points)))
        temporal_distance_split_widths = (
            distance_split_points_ordered[1:] - distance_split_points_ordered[:-1]
        )
        trip_counts = numpy.zeros(len(temporal_distance_split_widths))
        delta_peaks = defaultdict(lambda: 0)

        for block in self._profile_blocks:
            if block.distance_start == block.distance_end:
                delta_peaks[block.distance_end] += block.width()
            else:
                start_index = numpy.searchsorted(distance_split_points_ordered, block.distance_end)
                end_index = numpy.searchsorted(distance_split_points_ordered, block.distance_start)
                trip_counts[start_index:end_index] += 1

        unnormalized_cdf = numpy.array(
            [0] + list(numpy.cumsum(temporal_distance_split_widths * trip_counts))
        )
        if not (
            numpy.isclose(
                [unnormalized_cdf[-1]],
                [self._end_time - self._start_time - sum(delta_peaks.values())],
                atol=1e-4,
            ).all()
        ):
            print(
                unnormalized_cdf[-1], self._end_time - self._start_time - sum(delta_peaks.values())
            )
            raise RuntimeError("Something went wrong with cdf computation!")

        if len(delta_peaks) > 0:
            for peak in delta_peaks.keys():
                if peak == float("inf"):
                    continue
                index = numpy.nonzero(distance_split_points_ordered == peak)[0][0]
                unnormalized_cdf = numpy.insert(unnormalized_cdf, index, unnormalized_cdf[index])
                distance_split_points_ordered = numpy.insert(
                    distance_split_points_ordered, index, distance_split_points_ordered[index]
                )
                # walk_waiting_time_fraction = walk_total_time / (self.end_time_dep - self.start_time_dep)
                unnormalized_cdf[(index + 1) :] = (
                    unnormalized_cdf[(index + 1) :] + delta_peaks[peak]
                )

        norm_cdf = unnormalized_cdf / (unnormalized_cdf[-1] + delta_peaks[float("inf")])
        return distance_split_points_ordered, norm_cdf

    def _temporal_distance_pdf(self):
        """
        Temporal distance probability density function.

        Returns
        -------
        non_delta_peak_split_points: numpy.array
        non_delta_peak_densities: numpy.array
            len(density) == len(temporal_distance_split_points_ordered) -1
        delta_peak_loc_to_probability_mass : dict
        """
        temporal_distance_split_points_ordered, norm_cdf = self._temporal_distance_cdf()
        delta_peak_loc_to_probability_mass = {}

        non_delta_peak_split_points = [temporal_distance_split_points_ordered[0]]
        non_delta_peak_densities = []
        for i in range(0, len(temporal_distance_split_points_ordered) - 1):
            left = temporal_distance_split_points_ordered[i]
            right = temporal_distance_split_points_ordered[i + 1]
            width = right - left
            prob_mass = norm_cdf[i + 1] - norm_cdf[i]
            if width == 0.0:
                delta_peak_loc_to_probability_mass[left] = prob_mass
            else:
                non_delta_peak_split_points.append(right)
                non_delta_peak_densities.append(prob_mass / float(width))
        assert len(non_delta_peak_densities) == len(non_delta_peak_split_points) - 1
        return (
            numpy.array(non_delta_peak_split_points),
            numpy.array(non_delta_peak_densities),
            delta_peak_loc_to_probability_mass,
        )

    def get_vlines_and_slopes_for_plotting(self):
        vertical_lines = []
        slopes = []

        for i, block in enumerate(self._profile_blocks):
            distance_end_minutes = block.distance_end
            distance_start_minutes = block.distance_start

            slope = dict(
                x=[block.start_time, block.end_time],
                y=[distance_start_minutes, distance_end_minutes],
            )
            slopes.append(slope)

            if i != 0:
                # no vertical line for the first observation
                previous_duration_minutes = self._profile_blocks[i - 1].distance_end
                vertical_lines.append(
                    dict(
                        x=[block.start_time, block.start_time],
                        y=[previous_duration_minutes, distance_start_minutes],
                    )
                )
        return vertical_lines, slopes

    def get_blocks(self):
        return self._profile_blocks

    def interpolate(self, time):
        assert self._start_time <= time <= self._end_time
        for profile_block in self._profile_blocks:
            # find the first block whose end time is larger than or equal to that of the queried time
            if profile_block.end_time >= time:
                return profile_block.interpolate(time)
