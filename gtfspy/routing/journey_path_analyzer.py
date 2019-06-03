import datetime
from collections import Counter

from matplotlib import dates as md
from matplotlib import pyplot as plt
from pandas import DataFrame

from gtfspy.routing.node_profile_analyzer_time import NodeProfileAnalyzerTime
from gtfspy.routing.node_profile_analyzer_time_and_veh_legs import NodeProfileAnalyzerTimeAndVehLegs
from gtfspy.routing.label import LabelTimeBoardingsAndRoute, LabelTimeAndRoute
from gtfspy.routing.connection import Connection
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR
from gtfspy.smopy_plot_helper import legend_pt_modes
from gtfspy.routing.transfer_penalties import get_fastest_path_analyzer_after_transfer_penalties
from research.route_diversity.rd_utils import seconds_to_minutes


def if_df_empty_return_empty_list(apply_to_function):
    def wrapper(*args, **kwargs):
        try:
            func = apply_to_function(*args, **kwargs)
            return func
        except KeyError:
            return []
    return wrapper


class NodeJourneyPathAnalyzer(NodeProfileAnalyzerTimeAndVehLegs):

    """Subclass of NodeProfileAnalyzerTimeAndVehLegs, with extended support for route trajectories"""
    def __init__(self, labels, walk_to_target_duration, start_time_dep, end_time_dep, origin_stop,
                 transfer_penalty_seconds=0, gtfs=None):
        super().__init__(labels, walk_to_target_duration, start_time_dep, end_time_dep,
                         transfer_penalty_seconds=transfer_penalty_seconds)
        self.candidate_labels = self._get_labels_faster_than_walk()
        self.unpacked_df = None
        self.gtfs = gtfs
        self.origin_stop = origin_stop
        self.target_stop = None
        self.transfer_penalty_seconds = transfer_penalty_seconds
        self.fpa = self._get_fastest_path_analyzer()
        self.journey_boarding_stops = None
        self.journey_set_variants = None
        self.journey_waits = None
        self.variant_proportions = None
        self.pre_journey_waits = None
        self.unpack_fastest_path_journeys()
        self.processed_labels = None

    def _get_fastest_path_analyzer(self):
        self.fpa = get_fastest_path_analyzer_after_transfer_penalties(self.all_labels,
                                                                      self.start_time_dep,
                                                                      self.end_time_dep,
                                                                      walk_duration=self._walk_to_target_duration,
                                                                      label_props_to_consider=["n_boardings"],
                                                                      transfer_penalty_seconds=self.transfer_penalty_seconds)
        return self.fpa

    def unpack_fastest_path_journeys(self):
        self._unpack_journeys(self.candidate_labels)
        if not self.unpacked_df.empty:
            self.assign_path_letters()
            self.add_fastest_path_column()
        self._aggregate_time_weights()

    def fp_colname(self):
        return "fp_"+str(self.transfer_penalty_seconds)

    def add_fastest_path_column(self, transfer_penalty=None):
        if transfer_penalty:
            self.transfer_penalty_seconds = transfer_penalty
        fpa = self._get_fastest_path_analyzer()
        fp_labels = [(label.departure_time, label.n_boardings) for label in
                     fpa.get_labels_faster_than_walk()]
        self.unpacked_df[self.fp_colname()] = False
        self.unpacked_df.loc[self.unpacked_df["label_tuple"].isin(fp_labels), self.fp_colname()] = True
        return self.unpacked_df

    def _get_labels_faster_than_walk(self):
        return [x for x in self.all_labels if (x.arrival_time_target - x.departure_time) <=
                self._walk_to_target_duration]

    def _unpack_journeys(self, labels):
        """
        Back-tracks the complete journey by looking at the Connection object pointed to and the pointed previous label.
        Route outputs: boarding stops only, boarding and alighting stops, complete route
        Time outputs: aggregated in-vehicle, wait, walk -times
        Leg output: stores everything separately for each journey leg
        :param labels:
        :return:
        """
        if not labels:
            print("No labels for", self.origin_stop)
        labels = sorted(list(labels), key=lambda x: x.departure_time)
        target_stop = None
        unpacked_list = []
        for label in labels:
            assert (isinstance(label, LabelTimeAndRoute) or isinstance(label, LabelTimeBoardingsAndRoute))
            # We need to "unpack" the journey to actually figure out where the trip went
            # (there can be several targets).
            # TODO: look into why the weird label thing happens:
            if label.departure_time == label.arrival_time_target:
                print("Weird label:", label)
                continue

            origin_stop, target_stop, leg_value_list, boarding_stops, all_stops = self._collect_connection_data(label)
            if origin_stop == target_stop:
                continue

            unpacked_list.append({"connection_list": leg_value_list,
                                  "journey_boarding_stops": tuple(boarding_stops),
                                  "all_journey_stops": all_stops,
                                  "label_tuple": (label.departure_time, label.n_boardings),
                                  "label": label})

        self.target_stop = target_stop
        self.unpacked_df = DataFrame(unpacked_list)

        return self.unpacked_df

    @if_df_empty_return_empty_list
    def get_fp_connection_list(self):
        return self.unpacked_df["connection_list"].loc[self.unpacked_df[self.fp_colname()]].tolist()

    @if_df_empty_return_empty_list
    def get_fp_journey_boarding_stops(self):
        return self.unpacked_df["journey_boarding_stops"].loc[self.unpacked_df[self.fp_colname()]].tolist()

    @if_df_empty_return_empty_list
    def get_fp_all_journey_stops(self):
        return self.unpacked_df["all_journey_stops"].loc[self.unpacked_df[self.fp_colname()]].tolist()

    @if_df_empty_return_empty_list
    def get_original_fp_labels(self):
        return self.unpacked_df["label"].loc[self.unpacked_df[self.fp_colname()]].tolist()

    def get_modified_fp_labels(self):
        fpa = self._get_fastest_path_analyzer()
        return fpa.get_labels_faster_than_walk()

    @if_df_empty_return_empty_list
    def get_fp_path_letters(self):
        return self.unpacked_df["path_letters"].loc[self.unpacked_df[self.fp_colname()]].tolist()

    @staticmethod
    def _collect_connection_data(label):
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

    def assign_path_letters(self):
        """
        Function that assigns a littera for each journey variant that can be used in journey plots and temporal distance plots

        Parameters
        ----------
        features_to_check: frozenset

        Returns
        -------
        """
        variant_dict = {}

        def letter_generator():
            journey_letters = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            for f_letter in [""] + journey_letters:
                for s_letter in journey_letters:
                    yield f_letter+s_letter

        lg = letter_generator()
        for feature in self.unpacked_df["journey_boarding_stops"]:
            if feature not in variant_dict.keys():
                variant_dict[feature] = next(lg)

        self.unpacked_df["path_letters"] = self.unpacked_df.apply(lambda row: variant_dict[row.journey_boarding_stops],
                                                                  axis=1)

    def path_letters_for_stops(self):
        journey_boarding_stops = self.get_fp_journey_boarding_stops()
        path_letters = self.get_fp_path_letters()
        stop_dict = {}
        for variant, letter in zip(journey_boarding_stops, path_letters):
            for stop in variant:
                stop_dict.setdefault(stop, set())
                stop_dict[stop] = set.union(stop_dict[stop], {letter})
        return stop_dict

    def get_journey_trajectories(self):
        journeys = self.get_fp_connection_list()
        for journey in journeys:
            for leg in journey:
                lats, lons = zip(*[self.gtfs.get_stop_coordinates(x) for x in leg["leg_stops"]])
                _, leg_type = (None, -1) if leg["trip_id"] == -1 else self.gtfs.get_route_name_and_type_of_tripI(leg["trip_id"])
                yield lats, lons, leg_type

    def leg_generator(self, use_leg_stops=False):
        journeys = self.get_fp_connection_list()
        for journey in journeys:
            for leg in journey:
                dict_to_yield = {key: value for key, value in leg.items() if key in ["dep_stop", "arr_stop", "trip_id"]}
                if use_leg_stops:
                    leg_stops = [{"dep_stop": a, "arr_stop": b} for a, b in zip(leg["leg_stops"][:-1], leg["leg_stops"][1:])]
                    for leg_stop in leg_stops:
                        dict_to_yield.update(leg_stop)
                        yield dict_to_yield.copy()
                else:
                    yield dict_to_yield

    def _aggregate_time_weights(self, stop_tuple=None, walk_is_optimal_duration=None):
        """
        Collects the data needed for self.journey_set_variants, and self.variant_proportions, that is all journey
        variants and the proportion of those in time
        :param stop_tuple: list of tuples. Tuples of the boarding stop_Is describing trajectory of each included trip
        :param pre_journey_waits: list of ints
        :param walk_is_optimal_duration: int
        :return:
        """
        walk_tuple = tuple({self.origin_stop})
        weight_dict = {}

        if not self.pre_journey_waits:
            self.pre_journey_waits, walk_is_optimal_duration = self.fpa.calculate_pre_journey_waiting_times_to_list()

        if not self.pre_journey_waits and not walk_is_optimal_duration:
            self.journey_set_variants = None
            self.variant_proportions = None
            return

        elif not self.pre_journey_waits:
            weight_dict[walk_tuple] = walk_is_optimal_duration
        else:
            if stop_tuple:
                stop_tuple = [tuple(x) for x in stop_tuple]
            else:
                stop_tuple = self.get_fp_journey_boarding_stops()

            weight_dict = {x: 0 for x in set(stop_tuple)}
            for stop_set, pre_journey_wait in zip(stop_tuple, self.pre_journey_waits):
                weight_dict[stop_set] += pre_journey_wait
            if walk_is_optimal_duration > 0:
                weight_dict[walk_tuple] = weight_dict.get(walk_tuple, 0) + walk_is_optimal_duration

            # removal of journey variants without time weight
            weight_dict = {key: value for key, value in weight_dict.items() if value > 0}

        self.journey_set_variants = list(weight_dict.keys())
        self.variant_proportions = [x / sum(weight_dict.values()) for x in weight_dict.values()]

    def plot_journey_graph(self, ax, format_string="%H:%M:%S"):
        """
        for first leg, print start and end stop name, then only end stop. Departure and arrival time determine line
        trajectory, route type, the color
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
        route_types = set()
        for journey, letter in zip(self.get_fp_connection_list(), self.get_fp_path_letters()):
            for leg in journey:
                if leg["seq"] == 1:
                    prev_arr_time = None
                    y_level = _ut_to_unloc_datetime(leg["dep_time"])

                arr_stop_name = self.gtfs.get_name_from_stop_I(leg["arr_stop"])
                n_stops = len(leg["leg_stops"])
                dep_time, arr_time = _ut_to_unloc_datetime(leg["dep_time"]), _ut_to_unloc_datetime(leg["arr_time"])

                    #ax.text(dep_time + (prev_arr_time - dep_time)/2, y_level+1, "wait", fontsize=font_size, color="black")

                if not leg["trip_id"] == -1:
                    route_name, route_type = self.gtfs.get_route_name_and_type_of_tripI(leg["trip_id"])
                else:
                    route_name, route_type = "", -1

                route_types.add(route_type)
                if prev_arr_time and dep_time - prev_arr_time > datetime.timedelta(0) and route_type == -1:
                    walk_length = arr_time - dep_time
                    walk_end = prev_arr_time+walk_length
                    ax.plot([prev_arr_time, walk_end], [y_level, y_level], c=ROUTE_TYPE_TO_COLOR[route_type])
                    ax.plot([walk_end, arr_time], [y_level, y_level], ':', c=ROUTE_TYPE_TO_COLOR[-1])
                    route_types.add("wait")

                else:
                    if prev_arr_time and dep_time - prev_arr_time > datetime.timedelta(0):
                        ax.plot([prev_arr_time, dep_time], [y_level, y_level], ':', c=ROUTE_TYPE_TO_COLOR[-1])

                    ax.plot([dep_time, arr_time], [y_level, y_level], c=ROUTE_TYPE_TO_COLOR[route_type])
                    ax.text((arr_time + (dep_time - arr_time)/2), y_level - datetime.timedelta(seconds=30), route_name,
                            fontsize=font_size,
                            color="black", ha='center')

                    text_string = "for {0} stops".format(n_stops-2) if n_stops-2 > 1 else "for 1 stop" \
                        if n_stops-2 == 1 else ""
                    ax.text((arr_time + (dep_time - arr_time)/2), y_level + datetime.timedelta(seconds=70), text_string,
                            fontsize=font_size-2,
                            color="black", ha='center')

                if leg["seq"] == 1:
                    dep_stop_name = self.gtfs.get_name_from_stop_I(leg["dep_stop"])
                    ax.text(dep_time-datetime.timedelta(minutes=1), y_level, letter,
                            fontsize=font_size,
                            color="black", ha='right', va='center')
                prev_arr_time = arr_time
                wait_length = None

            # y_level += -15
        ax.set_ylim([_ut_to_unloc_datetime(self.end_time_dep), _ut_to_unloc_datetime(self.start_time_dep)])

        ax = legend_pt_modes(ax, route_types)
        x_axis_formatter = md.DateFormatter(format_string)
        ax.xaxis.set_major_formatter(x_axis_formatter)

        y_axis_formatter = md.DateFormatter(format_string)
        ax.yaxis.set_major_formatter(y_axis_formatter)

        # ax.axes.get_yaxis().set_visible(False)
        return ax

    def get_simple_diversities(self, measures=None):
        pm_calls = {"number_of_journey_variants": self.number_of_journey_variants,
                    "number_of_fp_journeys": self.number_of_fp_journeys,
                    "number_of_most_common_journey_variant": self.number_of_most_common_journey_variant,
                    "most_probable_journey_variant": self.most_probable_journey_variant,
                    "most_probable_departure_stop": self.most_probable_departure_stop,
                    "time_weighted_simpson": self.time_weighted_diversity,
                    "avg_circuity": self.avg_circuity,
                    "avg_speed": self.avg_journey_speed,
                    "mean_temporal_distance": self.mean_temporal_distance_minutes,
                    "mean_trip_n_boardings": self.mean_n_boardings_on_shortest_paths,
                    "largest_headway_gap": self.largest_headway_gap,
                    "expected_pre_journey_waiting_time": self.expected_pre_journey_waiting_time,
                    "proportion_fp_journeys": self.proportion_fp_journeys}
        if measures:
            pm_calls = {pm: value for pm, value in pm_calls.items() if pm in measures}
        if self.unpacked_df.empty:
            print("all labels rejected for stop", self.origin_stop)
            return {pm: None for pm in pm_calls.keys()}
        else:
            return {pm: value() for pm, value in pm_calls.items()}

    @seconds_to_minutes
    def largest_headway_gap(self):
        if self.pre_journey_waits:
            return max(self.pre_journey_waits)
        else:
            return None

    @seconds_to_minutes
    def expected_pre_journey_waiting_time(self):
        if not self.pre_journey_waits:
            return None
        elif max(self.pre_journey_waits) == 0:
            return 0
        else:
            proportions = [x/sum(self.pre_journey_waits) for x in self.pre_journey_waits]
            expected_waits = [x/2 for x in self.pre_journey_waits]
            return sum([x*y for x, y in zip(proportions, expected_waits)])

    def expected_waiting_time_at_most_probable_stop(self):
        pass

    def number_of_most_common_journey_variant(self):
        """
        starting data: origin stop, destination stop,
        journey trips -> trips using same trajectory
        :return:
        """
        return max(Counter(self.unpacked_df["path_letters"].tolist()).values())

    def proportion_fp_journeys(self):
        n_fp = self.number_of_fp_journeys()
        if n_fp == float("inf"):
            return 1
        elif n_fp is None:
            return None
        else:
            return n_fp / len(self.unpacked_df.index)

    @seconds_to_minutes
    def mean_temporal_distance_minutes(self):
        return self.mean_temporal_distance()

    def number_of_journey_variants(self):
        if not self.journey_set_variants:
            return None
        return len(self.journey_set_variants)

    def number_of_fp_journeys(self):
        if self._walk_to_target_duration < float("inf") and len(self.get_original_fp_labels()) == 0:
            return float("inf")
        elif self._walk_to_target_duration == float("inf") and len(self.get_original_fp_labels()) == 0:
            return None
        else:
            return len(self.get_original_fp_labels())

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

    def avg_journey_trajectory_length(self):
        journey_distances = []
        all_journey_stops = self.get_fp_all_journey_stops()
        if all_journey_stops:
            for journey_stops in all_journey_stops:
                distance = sum([self.gtfs.get_distance_between_stops_euclidean(prev_stop, stop)
                                for prev_stop, stop in zip(journey_stops, journey_stops[1:])])
                journey_distances.append(distance)
            return sum(journey_distances)/len(journey_distances)
        else:
            return None

    def avg_journey_duration(self):
        # TODO: decide if this should be avg journey duration, weighted avg jd or temporal distance
        journey_durations = []
        if self.get_original_fp_labels():
            for label in self.get_original_fp_labels():
                journey_durations.append(label.arrival_time_target - label.departure_time)
            return sum(journey_durations)/len(journey_durations)
        else:
            return None

    def avg_circuity(self):
        avg_journey_trajectory_length = self.avg_journey_trajectory_length()

        try:
            euclidean_distance = self.gtfs.get_distance_between_stops_euclidean(self.origin_stop, self.target_stop)
        except:
            euclidean_distance = None
            print("WARNING: INVALID STOP ID: ", self.origin_stop, self.target_stop)
        if not avg_journey_trajectory_length or not euclidean_distance:
            return None
        else:
            return avg_journey_trajectory_length/euclidean_distance

    def avg_journey_speed(self):
        avg_journey_trajectory_length = self.avg_journey_trajectory_length()
        avg_journey_duration = self.avg_journey_duration()
        if not avg_journey_trajectory_length or not avg_journey_duration:
            return None
        else:
            return 3.6*avg_journey_trajectory_length/avg_journey_duration

    def journey_variant_weighted_diversity(self):
        return self.simpson_diversity(stop_sets=self.get_fp_journey_boarding_stops())

    def time_weighted_diversity(self):
        return self.simpson_diversity(weights=self.variant_proportions)

    @staticmethod
    def simpson_diversity(stop_sets=None, weights=None):
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
            if not isinstance(stop_sets[0], tuple):
                stop_sets = [tuple(x) for x in stop_sets]
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
        stop_sets = [tuple(x) for x in stop_sets]
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

    def plot_fastest_temporal_distance_profile(self, timezone=None, **kwargs):
        if "ax" not in kwargs:
            fig = plt.figure(figsize=(10, 6))
            ax = fig.add_subplot(111)
            kwargs["ax"] = ax
        npat = NodeProfileAnalyzerTime(self.get_modified_fp_labels(), self._walk_to_target_duration,
                                       self.start_time_dep, self.end_time_dep)

        fig = npat.plot_temporal_distance_profile(timezone=timezone,
                                                  **kwargs)
        return fig
