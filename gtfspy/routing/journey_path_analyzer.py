import datetime
from matplotlib import dates as md
from gtfspy.routing.node_profile_analyzer_time_and_veh_legs import NodeProfileAnalyzerTimeAndVehLegs
from gtfspy.routing.label import LabelTimeBoardingsAndRoute, LabelTimeAndRoute
from gtfspy.routing.connection import Connection
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR


class NodeJourneyPathAnalyzer(NodeProfileAnalyzerTimeAndVehLegs):
    # TODO: possible measures: route diversity, circuity,

    """Subclass of NodeProfileAnalyzerTimeAndVehLegs, with extended support for route trajectories"""
    def __init__(self, labels, walk_to_target_duration, start_time_dep, end_time_dep, origin_stop):
        super().__init__(labels, walk_to_target_duration, start_time_dep, end_time_dep)
        self.fastest_path_labels = None
        self.gtfs = None
        self.origin_stop = origin_stop
        self.target_stop = None
        self.fpa = super(NodeJourneyPathAnalyzer, self)._get_fastest_path_analyzer()
        self.journey_boarding_stops = None
        self.journey_set_variants = None
        self.journey_waits = None
        self.variant_proportions = None
        self.walk_to_target_duration = walk_to_target_duration
        self.unpack_fastest_path_journeys()
        self.journey_letters, self.stop_dict = None, None

    def unpack_fastest_path_journeys(self):
        # TODO: generalized cost function
        if not self.fastest_path_labels:
            self.fastest_path_labels = self.fpa.get_labels_faster_than_walk()
        self._unpack_journeys(self.fastest_path_labels)
        self._aggregate_time_weights()

    def _unpack_journeys(self, labels):
        """
        Back-tracks the complete journey by looking at the Connection object pointed to and the pointed previous label.
        Route outputs: boarding stops only, boarding and alighting stops, complete route
        Time outputs: aggregated in-vehicle, wait, walk -times
        Leg output: stores everything separately for each journey leg
        :param labels:
        :return:
        """
        labels = sorted(list(labels), key=lambda x: x.departure_time)

        connection_list = []
        journey_boarding_stops = []
        all_journey_stops = []
        origin_stop = None
        target_stop = None
        all_stops = None
        for label in labels:
            assert (isinstance(label, LabelTimeAndRoute) or isinstance(label, LabelTimeBoardingsAndRoute))
            # We need to "unpack" the journey to actually figure out where the trip went
            # (there can be several targets).
            if label.departure_time == label.arrival_time_target:
                print("Weird label:", label)
                continue

            origin_stop, target_stop, leg_value_list, boarding_stops, all_stops = self._collect_connection_data(label)
            if origin_stop == target_stop:
                continue

            connection_list.append(leg_value_list)
            journey_boarding_stops.append(boarding_stops)
            all_journey_stops.append(all_stops)

        # self.origin_stop = origin_stop
        self.target_stop = target_stop
        self.journey_boarding_stops = [frozenset(x) for x in journey_boarding_stops]
        self.connection_list = connection_list
        self.all_journey_stops = all_journey_stops

    def _collect_connection_data(self, label):
        target_stop = None
        origin_stop = label.connection.departure_stop
        cur_label = label
        seq = 1
        leg_value_list = []
        boarding_stops = []
        leg_stops = []
        all_stops = []
        prev_trip_id = None
        connection = None
        leg_departure_time = None
        leg_departure_stop = None
        leg_arrival_time = None
        leg_arrival_stop = None
        while True:
            if isinstance(cur_label.connection, Connection):
                connection = cur_label.connection
                if connection.trip_id:
                    trip_id = connection.trip_id
                else:
                    trip_id = -1

                # In case of new leg
                if prev_trip_id != trip_id:
                    if not trip_id == -1:
                        boarding_stops.append(connection.departure_stop)
                    if prev_trip_id:
                        leg_stops.append(connection.departure_stop)

                        leg_values = {
                            "dep_stop": int(leg_departure_stop),
                            "arr_stop": int(leg_arrival_stop),
                            "dep_time": int(leg_departure_time),
                            "arr_time": int(leg_arrival_time),
                            "trip_id": int(prev_trip_id),
                            "seq": int(seq),
                            "leg_stops": [int(x) for x in leg_stops]
                        }
                        leg_value_list.append(leg_values)
                        seq += 1
                        leg_stops = []

                    leg_departure_stop = connection.departure_stop
                    leg_departure_time = connection.departure_time
                leg_arrival_time = connection.arrival_time
                leg_arrival_stop = connection.arrival_stop
                leg_stops.append(connection.departure_stop)
                all_stops.append(connection.departure_stop)
                target_stop = connection.arrival_stop
                prev_trip_id = trip_id

            if not cur_label.previous_label:
                leg_stops.append(connection.arrival_stop)
                all_stops.append(connection.arrival_stop)
                leg_values = {
                    "dep_stop": int(leg_departure_stop),
                    "arr_stop": int(leg_arrival_stop),
                    "dep_time": int(leg_departure_time),
                    "arr_time": int(leg_arrival_time),
                    "trip_id": int(prev_trip_id),
                    "seq": int(seq),
                    "leg_stops": [int(x) for x in leg_stops]
                }
                leg_value_list.append(leg_values)
                break

            cur_label = cur_label.previous_label
        boarding_stops = [int(x) for x in boarding_stops]
        return origin_stop, target_stop, leg_value_list, boarding_stops, all_stops

    def assign_path_letters(self, features_to_check):
        """
        Function that assigns a littera for each journey variant that can be used in journey plots and temporal distance plots
        :param features_to_check: frozenset
        :return:
        """
        variant_dict = {}

        def letter_generator():
            journey_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            for letter in journey_letters:
                yield letter

        lg = letter_generator()
        for feature in features_to_check:
            if feature not in variant_dict.keys():
                variant_dict[feature] = next(lg)

        stop_dict = {}
        for variant, letter in variant_dict.items():
            for stop in variant:
                try:
                    stop_dict[stop] += [letter]
                except KeyError:
                    stop_dict[stop] = [letter]
        self.journey_letters = [variant_dict[x] for x in features_to_check]
        self.stop_dict = stop_dict
        return self.journey_letters, self.stop_dict

    def get_journey_trajectories(self):
        journeys = self.connection_list

        for journey in journeys:
            for leg in journey:
                lats, lons = zip(*[self.gtfs.get_stop_coordinates(x) for x in leg["leg_stops"]])
                _, leg_type = (None, -1) if leg["trip_id"] == -1 else self.gtfs.get_route_name_and_type_of_tripI(leg["trip_id"])
                yield lats, lons, leg_type

    def get_pre_journey_waiting_times(self):
        pre_journey_waits, walk_time = self.fpa.calculate_pre_journey_waiting_times_to_list()
        return pre_journey_waits, walk_time

    def _aggregate_time_weights(self, stop_sets=None, pre_journey_waits=None, walk_is_optimal_duration=None):
        if not pre_journey_waits:
            pre_journey_waits, walk_is_optimal_duration = self.fpa.calculate_pre_journey_waiting_times_to_list()

        if stop_sets:
            stop_sets = [frozenset(x) for x in stop_sets]
        else:
            stop_sets = self.journey_boarding_stops

        if not pre_journey_waits and not walk_is_optimal_duration:
            self.journey_set_variants = None
            self.variant_proportions = None
            return

        weight_dict = {x: 0 for x in set(stop_sets)}
        for stop_set, pre_journey_wait in zip(stop_sets, pre_journey_waits):
            weight_dict[stop_set] += pre_journey_wait

        if walk_is_optimal_duration > 0:
            weight_dict[frozenset({self.origin_stop})] = walk_is_optimal_duration

        # removal of journey variants without time weight
        weight_dict = {key: value for key, value in weight_dict.items() if value > 0}

        self.journey_set_variants = list(weight_dict.keys())
        self.variant_proportions = [x / sum(weight_dict.values()) for x in weight_dict.values()]

    def plot_journey_graph(self, ax, format_string="%H:%M:%S"):
        """
        for first leg, print start and end stop name, then only end stop. Departure and arrival time determine line trajectory, route type, the color
        Print route name/or type over each line segment
        :return:
                        {
                    "dep_stop": int(leg_departure_stop),
                    "arr_stop": int(leg_arrival_stop),
                    "dep_time": int(leg_departure_time),
                    "arr_time": int(leg_arrival_time),
                    "trip_id": int(prev_trip_id),
                    "seq": int(seq),
                    "leg_stops": [int(x) for x in leg_stops]
                }
        """
        tz = self.gtfs.get_timezone_pytz()

        def _ut_to_unloc_datetime(ut):
            dt = datetime.datetime.fromtimestamp(ut, tz)
            return dt.replace(tzinfo=None)
        y_level = 0
        prev_arr_time = None
        font_size = 7
        wait_length = None
        for journey, letter in zip(self.connection_list, self.journey_letters):
            for leg in journey:
                if leg["seq"] == 1:
                    prev_arr_time = None

                arr_stop_name = self.gtfs.get_name_from_stop_I(leg["arr_stop"])
                n_stops = len(leg["leg_stops"])
                dep_time, arr_time = _ut_to_unloc_datetime(leg["dep_time"]), _ut_to_unloc_datetime(leg["arr_time"])



                    #ax.text(dep_time + (prev_arr_time - dep_time)/2, y_level+1, "wait", fontsize=font_size, color="black")

                if not leg["trip_id"] == -1:
                    route_name, route_type = self.gtfs.get_route_name_and_type_of_tripI(leg["trip_id"])
                else:
                    route_name, route_type = "", -1

                if prev_arr_time and dep_time - prev_arr_time > datetime.timedelta(0) and route_type == -1:
                    walk_length = arr_time - dep_time
                    walk_end = prev_arr_time+walk_length
                    ax.plot([prev_arr_time, walk_end], [y_level, y_level], c=ROUTE_TYPE_TO_COLOR[route_type])
                    ax.plot([walk_end, arr_time], [y_level, y_level], ':', c=ROUTE_TYPE_TO_COLOR[-1])

                else:
                    if prev_arr_time and dep_time - prev_arr_time > datetime.timedelta(0):
                        ax.plot([prev_arr_time, dep_time], [y_level, y_level], ':', c=ROUTE_TYPE_TO_COLOR[-1])

                    ax.plot([dep_time, arr_time], [y_level, y_level], c=ROUTE_TYPE_TO_COLOR[route_type])
                    ax.text((arr_time + (dep_time - arr_time)/2), y_level+0.5, route_name, fontsize=font_size,
                            color="black", ha='center')

                    text_string = "for {0} stops".format(n_stops-2) if n_stops-2 > 1 else "for 1 stop" if n_stops-2 == 1 else ""
                    ax.text((arr_time + (dep_time - arr_time)/2), y_level-font_size+1, text_string, fontsize=font_size-2,
                            color="black", ha='center')

                if leg["seq"] == 1:
                    dep_stop_name = self.gtfs.get_name_from_stop_I(leg["dep_stop"])
                    ax.text(dep_time-datetime.timedelta(minutes=1), y_level, letter,
                            fontsize=font_size,
                            color="black", ha='right', va='center')
                prev_arr_time = arr_time
                wait_length = None

            y_level += -15
        x_axis_formatter = md.DateFormatter(format_string)
        ax.xaxis.set_major_formatter(x_axis_formatter)
        return ax

    def get_simple_diversities(self):
        return {"number_of_journey_variants": self.number_of_journey_variants(),
                "number_of_fp_journeys": self.number_of_fp_journeys(),
                "most_probable_journey_variant": self.most_probable_journey_variant(),
                "most_probable_departure_stop": self.most_probable_departure_stop(),
                "journey_variant_weighted_simpson": self.journey_variant_simpson_diversity(stop_sets=self.journey_boarding_stops),
                "time_weighted_simpson": self.journey_variant_simpson_diversity(weights=self.variant_proportions)}

    def number_of_journey_variants(self):
        if not self.journey_set_variants:
            return None
        return len(self.journey_set_variants)

    def number_of_fp_journeys(self):
        if self.walk_to_target_duration < float("inf") and len(self.fastest_path_labels) == 0:
            return float("inf")
        elif self.walk_to_target_duration == float("inf") and len(self.fastest_path_labels) == 0:
            return None
        else:
            return len(self.fastest_path_labels)

    def most_probable_journey_variant(self):
        if not self.variant_proportions:
            return None
        return max(self.variant_proportions)

    def most_probable_departure_stop(self):
        if not self.journey_set_variants and not self.variant_proportions:
            return None
        stop_dict = {list(x)[0]: 0 for x in self.journey_set_variants}
        for stop_set, proportion in zip(self.journey_set_variants, self.variant_proportions):
            stop_dict[list(stop_set)[0]] += proportion
        return max(stop_dict.values())

    @staticmethod
    def journey_variant_simpson_diversity(stop_sets=None, weights=None):
        """
        Diversity measure that takes into account the number of trip variants and
        the distribution of trips by variant.
        :param weights:
        :param stop_sets:
        :return:
        """
        if not stop_sets and not weights:
            return None

        assert (stop_sets and not weights) or (not stop_sets and weights)
        if not weights:
            if not isinstance(stop_sets[0], frozenset):
                stop_sets = [frozenset(x) for x in stop_sets]
            weight_dict = {x: stop_sets.count(x) for x in set(stop_sets)}
            weights = [weight_dict[x]/sum(weight_dict.values()) for x in weight_dict.keys()]

        return sum([x*x for x in weights])

    @staticmethod
    def stop_probability_diversity(stop_sets, weights=None):
        """
        A diversity measure that takes into account the number of trip variants,
        the distribution of trips by variant and the similarity of the trip variants
        :param weights:
        :param stop_sets:
        :return:
        """
        stop_sets = [frozenset(x) for x in stop_sets]
        if not weights:
            weight_dict = {x: stop_sets.count(x)/len(stop_sets) for x in set(stop_sets)}
            weights = [weight_dict[x] for x in stop_sets]
            stop_sets = list(set(stop_sets))
        all_stops = set([item for sublist in stop_sets for item in sublist])
        stop_dict = {str(x): {"stop_p": 0, "journey_p": 0} for x in all_stops}
        for weight, stop_set in zip(weights, stop_sets):
            n_stops = len(stop_set)
            for stop in stop_set:
                stop_dict[str(stop)]["stop_p"] += 1/n_stops*weight
                stop_dict[str(stop)]["journey_p"] += weight

        spd = sum([x["stop_p"]*x["journey_p"] for key, x in stop_dict.items()])
        return spd

    @staticmethod
    def journey_independence(routes, gtfs, return_average=True):
        segment_dict = {}
        journey_segment_list = []
        for route in routes:
            prev_stop = None
            journey_segments = []
            for stop in route:
                if prev_stop:
                    if (prev_stop, stop) in segment_dict.keys():
                        segment_dict[(prev_stop, stop)]["journey_overlap"] += 1
                    else:
                        segment_dict[(prev_stop, stop)] = \
                            {"distance": gtfs.get_distance_between_stops_euclidean(prev_stop, stop),
                             "journey_overlap": 1}

                    journey_segments.append((prev_stop, stop))

            journey_segment_list.append(journey_segments)
        independence_levels = []
        for journey_segments in journey_segment_list:
            total_distance = sum([segment_dict[x]["distance"] for x in journey_segments])
            weighted_sum = sum([segment_dict[x]["distance"]/segment_dict[x]["journey_overlap"] for x in journey_segments])
            independence_levels.append(total_distance*weighted_sum)
        if return_average:
            return sum(independence_levels)/len(independence_levels)
        else:
            return independence_levels

    @staticmethod
    def sorensen_and_simpson_diversity(sets):
        """
        New journey diversity measure based on jaccard/sörensen's index
        Parameters
        ----------
        sets: list of sets
        :return:
        """
        import itertools
        if len(sets) < 2:
            return 0
        else:
            a = sum([len(x) for x in sets])-len(set.union(*sets))
            b = sum([min(len(i-j), len(j-i)) for i, j in itertools.combinations(sets, 2)])
            c = sum([max(len(i-j), len(j-i)) for i, j in itertools.combinations(sets, 2)])
            sor = (b+c)/(2*a+b+c)
            sim = b/(a+b)

            return sor, sim
