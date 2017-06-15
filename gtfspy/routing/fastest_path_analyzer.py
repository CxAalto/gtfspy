from gtfspy.routing.label import compute_pareto_front
from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime
from gtfspy.routing.profile_block_analyzer import ProfileBlock, ProfileBlockAnalyzer


class FastestPathAnalyzer:

    def __init__(self, labels, start_time_dep, end_time_dep, cutoff_duration=float('inf'), label_props_to_consider=None, **kwargs):
        """
        Parameters
        ----------
        labels: list
            List of labels (each label should at least have attributes "departure_time" and "arrival_time")
        cutoff_duration: float
            What is the maximum duration for a journey to be considered.
        label_props_to_consider: list
        """
        for label in labels:
            assert (hasattr(label, "departure_time"))
            assert (hasattr(label, "arrival_time_target"))
        self.start_time_dep = start_time_dep
        self.end_time_dep = end_time_dep
        self.cutoff_duration = cutoff_duration
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
        labels_within_interval = [label for label in labels if
                           (self.start_time_dep <= label.departure_time <= self.end_time_dep)]
        final_labels = list(reversed(compute_pareto_front(labels_within_interval, ignore_n_boardings=True)))
        # assert ordered:
        for i in range(len(final_labels) - 1):
            assert (final_labels[i].departure_time <= final_labels[i + 1].departure_time)

        if len(final_labels) is 0 or final_labels[-1].departure_time < self.end_time_dep:
            # add an after label
            smallest_dep_time_after_end_time = float('inf')
            smallest_dep_time_label = None
            for label in labels:
                if self.end_time_dep < label.departure_time < smallest_dep_time_after_end_time:
                    smallest_dep_time_after_end_time = label.departure_time
                    smallest_dep_time_label = label
            if smallest_dep_time_label is not None:
                final_labels.append(smallest_dep_time_label)
        return final_labels

    def get_fastest_path_labels(self, include_next_label_outside_interval=False):
        if include_next_label_outside_interval:
            return self._fastest_path_labels
        else:
            if self._fastest_path_labels[-1].departure_time == self.end_time_dep:
                return self._fastest_path_labels
            else:
                return self._fastest_path_labels[:-1]


    def get_fastest_path_blocks(self):
        """
        Returns
        -------
        blocks: list[ProfileBlock]
        """
        def _label_to_prop_dict(label):
            return {prop:getattr(label, prop) for prop in self.label_props}

        labels = self._fastest_path_labels
        for i in range(len(labels) - 1):
            assert (labels[i].departure_time <= labels[i + 1].departure_time)

        previous_dep_time = self.start_time_dep
        blocks = []
        for label in labels:
            if previous_dep_time >= self.end_time_dep:
                break
            end_time = min(label.departure_time, self.end_time_dep)
            assert (end_time >= previous_dep_time)
            distance_start = label.duration() + (label.departure_time - previous_dep_time)
            if distance_start > self.cutoff_duration:
                split_point_x_computed = label.departure_time - (self.cutoff_duration - label.duration())
                split_point_x = min(split_point_x_computed, end_time)
                walk_block = ProfileBlock(previous_dep_time,
                                          split_point_x,
                                          self.cutoff_duration,
                                          self.cutoff_duration)
                assert (previous_dep_time <= split_point_x)
                blocks.append(walk_block)
                if split_point_x < end_time:
                    assert (split_point_x <= end_time)
                    trip_block = ProfileBlock(split_point_x, end_time,
                                              label.duration() + (end_time - split_point_x),
                                              label.duration(),
                                              **_label_to_prop_dict(label))
                    blocks.append(trip_block)
            else:
                journey_block = ProfileBlock(
                    previous_dep_time,
                    end_time,
                    distance_start - (end_time - previous_dep_time),
                    distance_start,
                    **_label_to_prop_dict(label))
                blocks.append(journey_block)
            previous_dep_time = blocks[-1].end_time

        if previous_dep_time < self.end_time_dep:
            last_block = ProfileBlock(previous_dep_time,
                                      self.end_time_dep,
                                      self.cutoff_duration,
                                      self.cutoff_duration)
            blocks.append(last_block)
        return blocks

    def get_time_analyzer(self):
        """
        Returns
        -------
        NodeProfileAnalyzerTime
        """
        raise NotImplementedError

    def get_props(self):
        return list(self.label_props)

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
        fp_blocks = self.get_fastest_path_blocks()
        prop_blocks = []
        for b in fp_blocks:
            if b.is_flat():
                if b.distance_end == self.cutoff_duration:
                    prop_value = value_cutoff
                else:
                    prop_value = value_no_next_journey
            else:
                prop_value = b[property]
            prop_block = ProfileBlock(b.start_time, b.end_time, prop_value, prop_value)
            prop_blocks.append(prop_block)
        return ProfileBlockAnalyzer(prop_blocks, **kwargs)



