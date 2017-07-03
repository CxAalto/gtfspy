from gtfspy.routing.label import compute_pareto_front
from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime
from gtfspy.routing.profile_block_analyzer import ProfileBlock, ProfileBlockAnalyzer


class FastestPathAnalyzer:

    def __init__(self, labels, start_time_dep, end_time_dep, walk_duration=float('inf'), label_props_to_consider=None,
                 **kwargs):
        """
        Parameters
        ----------
        labels: list
            List of labels (each label should at least have attributes "departure_time" and "arrival_time")
        walk_duration: float
            What is the maximum duration for a journey to be considered.
        label_props_to_consider: list
        """
        for label in labels:
            assert (hasattr(label, "departure_time"))
            assert (hasattr(label, "arrival_time_target"))
        self.start_time_dep = start_time_dep
        self.end_time_dep = end_time_dep
        self.walk_duration = walk_duration
        if label_props_to_consider is None:
            self.label_props = []
        else:
            self.label_props = label_props_to_consider
        self._fastest_path_labels = self._compute_fastest_path_labels(labels)
        # assert each label has the required properties
        for label in self._fastest_path_labels:
            for prop in self.label_props:
                assert (hasattr(label, prop))

        self.kwargs = kwargs

    def _compute_fastest_path_labels(self, labels):
        fp_labels = [label for label in labels if
                           (self.start_time_dep < label.departure_time <= self.end_time_dep)]
        if len(fp_labels) is 0 or fp_labels[-1].departure_time < self.end_time_dep:
            # add an after label
            smallest_arr_time_after_end_time = float('inf')
            smallest_arr_time_label = None
            for label in labels:
                if self.end_time_dep < label.departure_time and (label.arrival_time_target < smallest_arr_time_after_end_time):
                    smallest_arr_time_after_end_time = label.arrival_time_target
                    smallest_arr_time_label = label
            if smallest_arr_time_label is not None:
                fp_labels.append(smallest_arr_time_label)

        fp_labels = list(reversed(compute_pareto_front(fp_labels, ignore_n_boardings=True)))
        # assert ordered:
        for i in range(len(fp_labels) - 1):
            assert (fp_labels[i].departure_time < fp_labels[i + 1].departure_time)

        return fp_labels

    def get_fastest_path_labels(self, include_next_label_outside_interval=False):
        if include_next_label_outside_interval:
            return self._fastest_path_labels
        else:
            if self._fastest_path_labels[-1].departure_time == self.end_time_dep:
                return self._fastest_path_labels
            else:
                return self._fastest_path_labels[:-1]

    def calculate_pre_journey_waiting_times(self):
        previous_label = None
        for label in self._fastest_path_labels:
            if previous_label:
                label.pre_journey_wait_fp = label.departure_time - previous_label.departure_time
            else:
                label.pre_journey_wait_fp = label.departure_time - self.start_time_dep
            previous_label = label

    def get_fastest_path_temporal_distance_blocks(self):
        """
        Returns
        -------
        blocks: list[ProfileBlock]
        """
        def _label_to_prop_dict(label):
            return {prop: getattr(label, prop) for prop in self.label_props}

        labels = self._fastest_path_labels
        for i in range(len(labels) - 1):
            assert (labels[i].departure_time < labels[i + 1].departure_time)

        previous_dep_time = self.start_time_dep
        blocks = []
        for label in labels:
            if previous_dep_time >= self.end_time_dep:
                break
            end_time = min(label.departure_time, self.end_time_dep)
            assert (end_time >= previous_dep_time)

            temporal_distance_start = label.duration() + (label.departure_time - previous_dep_time)

            if temporal_distance_start > self.walk_duration:
                split_point_x_computed = label.departure_time - (self.walk_duration - label.duration())
                split_point_x = min(split_point_x_computed, end_time)
                if previous_dep_time < split_point_x:
                    # add walk block, only if it is required
                    walk_block = ProfileBlock(previous_dep_time,
                                              split_point_x,
                                              self.walk_duration,
                                              self.walk_duration,
                                              **_label_to_prop_dict(label))
                    blocks.append(walk_block)
                if split_point_x < end_time:
                    trip_block = ProfileBlock(split_point_x, end_time,
                                              label.duration() + (end_time - split_point_x),
                                              label.duration(),
                                              **_label_to_prop_dict(label))
                    blocks.append(trip_block)
            else:
                journey_block = ProfileBlock(
                    previous_dep_time,
                    end_time,
                    temporal_distance_start,
                    temporal_distance_start - (end_time - previous_dep_time),
                    **_label_to_prop_dict(label))
                blocks.append(journey_block)
            previous_dep_time = blocks[-1].end_time

        if previous_dep_time < self.end_time_dep:
            last_block = ProfileBlock(previous_dep_time,
                                      self.end_time_dep,
                                      self.walk_duration,
                                      self.walk_duration)
            blocks.append(last_block)
        return blocks

    def get_time_analyzer(self):
        """
        Returns
        -------
        NodeProfileAnalyzerTime
        """
        return NodeProfileAnalyzerTime(self._fastest_path_labels,
                                       self.walk_duration,
                                       self.start_time_dep,
                                       self.end_time_dep)

    def get_props(self):
        return list(self.label_props)

    def get_temporal_distance_analyzer(self):
        kwargs = self.kwargs
        return ProfileBlockAnalyzer(self.get_fastest_path_temporal_distance_blocks(), **kwargs)

    def get_prop_analyzer_flat(self, property, value_no_next_journey, value_cutoff):
        """
        Get a journey property analyzer, where each journey is weighted by the number of.

        Parameters
        ----------
        property: string
            Name of the property, needs to be one of label_props given on initialization.
        value_no_next_journey:
            Value of the profile, when there is no next journey available.
        value_cutoff: number
            default value of the property when cutoff is applied

        Returns
        -------
        ProfileBlockAnalyzer
        """
        kwargs = self.kwargs
        fp_blocks = self.get_fastest_path_temporal_distance_blocks()
        prop_blocks = []
        for b in fp_blocks:
            if b.is_flat():
                if b.distance_end == self.walk_duration:
                    prop_value = value_cutoff
                else:
                    prop_value = value_no_next_journey
            else:
                prop_value = b[property]
            prop_block = ProfileBlock(b.start_time, b.end_time, prop_value, prop_value)
            prop_blocks.append(prop_block)
        return ProfileBlockAnalyzer(prop_blocks, **kwargs)



