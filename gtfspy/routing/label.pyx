cdef class LabelTimeSimple:
    """
    LabelTime describes entries in a Profile.
    """
    cdef:
        public double departure_time
        public double arrival_time_target

    def __init__(self, double departure_time=-float("inf"), double arrival_time_target=float('inf')):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target

    def __richcmp__(LabelTimeSimple self, LabelTimeSimple other, int op):
        self_tuple = self.departure_time, -self.arrival_time_target
        other_tuple = other.departure_time, -other.arrival_time_target
        if op == 2:  # ==
            return self_tuple == other_tuple
        if op == 3:  # !=
            return self_tuple != other_tuple
        if op == 0:  # less than
            return self_tuple < other_tuple
        elif op == 4:  # greater than
            return self_tuple > other_tuple
        elif op == 1:  # <=
            return self_tuple <= other_tuple
        elif op == 5:  # >=
            return self_tuple >= other_tuple

    # __getstate__ and __setstate__ : for pickling
    def __getstate__(self):
        return self.departure_time, self.arrival_time_target

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target = state

    cpdef int dominates(self, LabelTimeSimple other) except *:
        return self.departure_time >= other.departure_time and self.arrival_time_target <= other.arrival_time_target

    cpdef int dominates_ignoring_dep_time(self, LabelTimeSimple other):
        return self.arrival_time_target <= other.arrival_time_target

    @staticmethod
    def direct_walk_label(double departure_time, double walk_duration):
        return LabelTimeSimple(departure_time, departure_time + walk_duration)

    cpdef double duration(self):
        return self.arrival_time_target - self.departure_time

    cpdef LabelTimeSimple get_copy(self):
        return LabelTimeSimple(self.departure_time, self.arrival_time_target)

cdef class LabelTime:
    """
    LabelTime describes entries in a Profile.
    """
    cdef:
        public double departure_time
        public double arrival_time_target
        public bint first_leg_is_walk

    def __init__(self, double departure_time=-float("inf"), double arrival_time_target=float('inf'), bint first_leg_is_walk=False,
                 **kwargs):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target
        self.first_leg_is_walk = first_leg_is_walk

    cpdef _tuple_for_ordering(self):
        return self.departure_time, -self.arrival_time_target, not self.first_leg_is_walk

    def __richcmp__(LabelTime self, LabelTime other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2:  # ==
            return self_tuple == other_tuple
        if op == 3:  # !=
            return self_tuple != other_tuple
        if op == 0:  # less than
            return self_tuple < other_tuple
        elif op == 4:  # greater than
            return self_tuple > other_tuple
        elif op == 1:  # <=
            return self_tuple <= other_tuple
        elif op == 5:  # >=
            return self_tuple >= other_tuple

    # __getstate__ and __setstate__ : for pickling
    def __getstate__(self):
        return self.departure_time, self.arrival_time_target, self.first_leg_is_walk

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target, self.first_leg_is_walk = state

    cpdef int dominates(LabelTime self, LabelTime other) except *:
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        return all([(s >= o) for s, o in zip(self_tuple, other_tuple)])

    cpdef int dominates_ignoring_dep_time(LabelTime self, LabelTime other):
        return self.arrival_time_target <= other.arrival_time_target and self.first_leg_is_walk <= other.first_leg_is_walk

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelTime other):
        return self.arrival_time_target <= other.arrival_time_target

    cpdef LabelTime get_copy(self):
        return LabelTime(self.departure_time, self.arrival_time_target, self.first_leg_is_walk)

    cpdef LabelTime get_copy_with_specified_departure_time(self, double departure_time):
        return LabelTime(departure_time, self.arrival_time_target, self.first_leg_is_walk)

    @staticmethod
    def direct_walk_label(double departure_time, double walk_duration):
        return LabelTime(departure_time, departure_time + walk_duration, True)

    cpdef double duration(self):
        return self.arrival_time_target - self.departure_time

    cpdef LabelTime get_copy_with_walk_added(self, walk_duration):
        return LabelTime(self.departure_time - walk_duration, self.arrival_time_target, self.first_leg_is_walk)

cdef class LabelTimeWithBoardingsCount:
    cdef:
        public double departure_time
        public double arrival_time_target
        public int n_boardings
        public bint first_leg_is_walk


    def __init__(self, double departure_time, double arrival_time_target,
                 int n_boardings, bint first_leg_is_walk):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target
        self.n_boardings = n_boardings
        self.first_leg_is_walk = first_leg_is_walk

    def __getstate__(self):
        return self.departure_time, self.arrival_time_target, self.n_boardings

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target, self.n_boardings = state

    def _tuple_for_ordering(self):
        return self.departure_time, -self.arrival_time_target, -self.n_boardings, not self.first_leg_is_walk

    def __richcmp__(LabelTimeWithBoardingsCount self, LabelTimeWithBoardingsCount other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2:  # ==
            return self_tuple == other_tuple
        if op == 3:  # !=
            return self_tuple != other_tuple
        if op == 0:  # less than
            return self_tuple < other_tuple
        elif op == 4:  # greater than
            return self_tuple > other_tuple
        elif op == 1:  # <=
            return self_tuple <= other_tuple
        elif op == 5:  # >=
            return self_tuple >= other_tuple

    cpdef int dominates(self, LabelTimeWithBoardingsCount other):
        """
        Compute whether this LabelWithNumberVehicles dominates the other LabelWithNumberVehicles

        Parameters
        ----------
        other: LabelTimeWithBoardingsCount

        Returns
        -------
        dominates: bint
            True if this ParetoTuple dominates the other, otherwise False
        """
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        return not any([(s < o) for s, o in zip(self_tuple, other_tuple)])

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelTimeWithBoardingsCount other):
        dominates = (
            self.arrival_time_target <= other.arrival_time_target and
            self.n_boardings <= other.n_boardings
        )
        return dominates

    cpdef int dominates_ignoring_dep_time(self, LabelTimeWithBoardingsCount other):
        cdef:
            int dominates
        dominates = (
            self.arrival_time_target <= other.arrival_time_target and
            self.n_boardings <= other.n_boardings and
            self.first_leg_is_walk <= other.first_leg_is_walk
        )
        return dominates

    cpdef int dominates_ignoring_time(self, LabelTimeWithBoardingsCount other):
        cdef:
            int dominates
        dominates = (
            self.n_boardings <= other.n_boardings and
            self.first_leg_is_walk <= other.first_leg_is_walk
        )
        return dominates

    cpdef int dominates_ignoring_dep_time_and_n_boardings(self, LabelTimeWithBoardingsCount other):
        cdef:
            int dominates
        dominates = (
            self.arrival_time_target <= other.arrival_time_target and
            self.first_leg_is_walk <= other.first_leg_is_walk
        )
        return dominates

    cpdef get_copy(self):
        return LabelTimeWithBoardingsCount(self.departure_time, self.arrival_time_target,
                                           self.n_boardings, self.first_leg_is_walk)

    cpdef get_copy_with_specified_departure_time(self, departure_time):
        return LabelTimeWithBoardingsCount(departure_time, self.arrival_time_target,
                                           self.n_boardings, self.first_leg_is_walk)

    cpdef double duration(self):
        return self.arrival_time_target - self.departure_time

    @staticmethod
    def direct_walk_label(departure_time, walk_duration):
        return LabelTimeWithBoardingsCount(departure_time, departure_time + walk_duration, 0, True)

    cpdef LabelTimeWithBoardingsCount get_copy_with_walk_added(self, double walk_duration):
        return LabelTimeWithBoardingsCount(self.departure_time - walk_duration,
                                           self.arrival_time_target, self.n_boardings, True)

    def __str__(self):
        return str((self.departure_time, self.arrival_time_target, self.n_boardings, self.first_leg_is_walk))


cdef class LabelVehLegCount:
    cdef:
        public double departure_time
        public int n_boardings
        public bint first_leg_is_walk

    def __init__(self, n_boardings=0, double departure_time=-float('inf'), first_leg_is_walk=False, **kwargs):
        self.n_boardings = n_boardings
        self.departure_time = departure_time
        self.first_leg_is_walk = first_leg_is_walk

    def __getstate__(self):
        return self.departure_time, self.n_boardings, self.first_leg_is_walk

    def __setstate__(self, state):
        self.departure_time, self.n_boardings, self.first_leg_is_walk = state

    def _tuple_for_ordering(self):
        return -self.n_boardings, self.departure_time, not self.first_leg_is_walk

    def __richcmp__(LabelVehLegCount self, LabelVehLegCount other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2:
            return self_tuple == other_tuple
        if op == 3:
            return self_tuple != other_tuple
        if op == 0:  # less than
            return self_tuple < other_tuple
        elif op == 4:  # greater than
            return self_tuple > other_tuple
        elif op == 1:  # <=
            return self_tuple <= other_tuple
        elif op == 5:  # >=
            return self_tuple >= other_tuple

    cpdef int dominates(self, LabelVehLegCount other) except *:
        """
        Compute whether this LabelWithNumberVehicles dominates the other LabelWithNumberVehicles

        Parameters
        ----------
        other: LabelTimeWithBoardingsCount

        Returns
        -------
        dominates: bint
            True if this ParetoTuple dominates the other, otherwise False
        """
        return self.n_boardings <= other.n_boardings and self.first_leg_is_walk <= other.first_leg_is_walk

    cpdef int dominates_ignoring_dep_time(self, LabelVehLegCount other):
        return self.dominates(other)

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelVehLegCount other):
        return self.n_boardings <= other.n_boardings

    def get_copy(self):
        return LabelVehLegCount(self.n_boardings, first_leg_is_walk=self.first_leg_is_walk)

    def get_copy_with_specified_departure_time(self, departure_time):
        return LabelVehLegCount(self.n_boardings, departure_time, self.first_leg_is_walk)

    def get_copy_with_walk_added(self, walk_duration):
        return LabelVehLegCount(self.n_boardings, departure_time=self.departure_time - walk_duration,
                                first_leg_is_walk=True)

    @staticmethod
    def direct_walk_label(departure_time, walk_duration):
        return LabelVehLegCount(0, departure_time, True)

# ctypedef fused label:
#     LabelTime
#     LabelTimeWithBoardingsCount
#     LabelVehLegCount
#
# cpdef int dominates(label first, label other):
#     return first.dominates(other)

def compute_pareto_front_smart(list label_list):
    return compute_pareto_front(label_list)

def compute_pareto_front(list label_list, finalization=False, ignore_n_boardings=False):
    pareto_front = []
    if len(label_list) == 0:
        return pareto_front

    assert(not (finalization and ignore_n_boardings))
    # determine function used for domination:
    label = next(iter(label_list))
    if finalization:
        dominates = label.__class__.dominates_ignoring_dep_time_finalization
    elif ignore_n_boardings:
        dominates = label.__class__.dominates_ignoring_dep_time_and_n_boardings
    else:
        dominates = label.__class__.dominates_ignoring_dep_time


    label_list = list(reversed(sorted(label_list)))  # n log(n)
    # assume only that label_list is sorted by departure time (best last)
    current_best_labels_wo_deptime = []
    for new_label in label_list:  # n times
        is_dominated = False
        for best_label in current_best_labels_wo_deptime:
            # the size of current_best_labels_wo_deptime should remain small
            # the new_label can dominate the old ones only partially
            # check if the new one is dominated by the old ones ->
            if dominates(best_label, new_label):
                is_dominated = True
                break
        if is_dominated:
            continue  # do nothing
        else:
            pareto_front.append(new_label)
            new_best = []
            for old_partial_best in current_best_labels_wo_deptime:
                if not dominates(new_label, old_partial_best):
                    new_best.append(old_partial_best)
            new_best.append(new_label)
            current_best_labels_wo_deptime = new_best
    return pareto_front

def compute_pareto_front_naive(list label_list):
    """
    Computes the Pareto frontier of a given label_list

    Parameters
    ----------
    label_list: list[LabelTime]
        (Or any list of objects, for which a function label.dominates(other) is defined.

    Returns
    -------
    pareto_front: list[LabelTime]
        List of labels that belong to the Pareto front.

    Notes
    -----
    Code adapted from:
    http://stackoverflow.com/questions/32791911/fast-calculation-of-pareto-front-in-python
    """
    # cdef:
    #    list dominated
    #    list pareto_front
    #    list remaining
    #    Label other
    #    Label candidate
    dominated = []
    pareto_front = []
    remaining = label_list
    while remaining:  # (is not empty)
        candidate = remaining[0]
        new_remaining = []
        is_dominated = False
        for other_i in range(1, len(remaining)):
            other = remaining[other_i]
            if candidate.dominates(other):
                dominated.append(other)
            else:
                new_remaining.append(other)
                if other.dominates(candidate):
                    is_dominated = True
        if is_dominated:
            dominated.append(candidate)
        else:
            pareto_front.append(candidate)
        remaining = new_remaining
        # after each round:
        #   remaining contains Labels that are not dominated by any in the pareto_front
        #   dominated contains Labels that are dominated by some other LabelTime
    return pareto_front

def merge_pareto_frontiers(labels, labels_other):
    """
    Merge two pareto frontiers by removing dominated entries.

    Parameters
    ----------
    labels: list[LabelTime]
    labels_other: list[LabelTime]

    Returns
    -------
    pareto_front_merged: list[LabelTime]
    """
    def _get_non_dominated_entries(candidates, possible_dominators, survivor_list=None):
        if survivor_list is None:
            survivor_list = list()
        for candidate in candidates:
            candidate_is_dominated = False
            for dominator in possible_dominators:
                if dominator.dominates(candidate):
                    candidate_is_dominated = True
                    break
            if not candidate_is_dominated:
                survivor_list.append(candidate)
        return survivor_list

    survived = []
    survived = _get_non_dominated_entries(labels, labels_other, survivor_list=survived)
    survived = _get_non_dominated_entries(labels_other, survived, survivor_list=survived)
    return survived

def min_arrival_time_target(label_list):
    if len(label_list) > 0:
        return min(label_list, key=lambda label: label.arrival_time_target).arrival_time_target
    else:
        return float('inf')

def min_n_vehicle_trips(label_list):
    if len(label_list) > 0:
        return min(label_list, key=lambda label: label.n_vehicle_trips).n_vehicle_trips
    else:
        return None

cdef class LabelTimeBoardingsAndRoute:
    # Label with implemented added constraint for cases when two labels are tied:
    # The trip with minimal "movement time" should be chosen = maximizing the waiting time for robustness
    cdef:
        public double departure_time
        public double arrival_time_target
        public int n_boardings
        public int movement_duration
        public bint first_leg_is_walk
        public object previous_label
        public object connection


    def __init__(self, double departure_time, double arrival_time_target,
                 int n_boardings, int movement_duration, bint first_leg_is_walk, object connection=None, object previous_label=None):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target
        self.n_boardings = n_boardings
        self.movement_duration = movement_duration
        self.first_leg_is_walk = first_leg_is_walk
        self.previous_label = previous_label
        self.connection = connection

    def __getstate__(self):
        return self.departure_time, self.arrival_time_target, self.n_boardings, self.movement_duration, self.connection, self.previous_label

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target, self.n_boardings, self.movement_duration, self.connection, self.previous_label = state

    def _tuple_for_ordering(self):
        return self.departure_time, -self.arrival_time_target, -self.n_boardings, not self.first_leg_is_walk, -self.movement_duration

    def __richcmp__(LabelTimeBoardingsAndRoute self, LabelTimeBoardingsAndRoute other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2:  # ==
            return self_tuple == other_tuple
        if op == 3:  # !=
            return self_tuple != other_tuple
        if op == 0:  # less than
            return self_tuple < other_tuple
        elif op == 4:  # greater than
            return self_tuple > other_tuple
        elif op == 1:  # <=
            return self_tuple <= other_tuple
        elif op == 5:  # >=
            return self_tuple >= other_tuple

    cpdef int dominates(self, LabelTimeBoardingsAndRoute other):
        """
        Compute whether this LabelTimeBoardingsAndRoute dominates the other LabelTimeBoardingsAndRoute
        Parameters
        ----------
        other: LabelTimeBoardingsAndRoute
        Returns
        -------
        dominates: bint
            True if this ParetoTuple dominates the other, otherwise False
        """
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if any([(s < o) for s, o in zip(self_tuple[:-1], other_tuple[:-1])]):
            return False
        elif all([(s == o) for s, o in zip(self_tuple[:-1], other_tuple[:-1])]) and self_tuple[-1] < other_tuple[-1]:
            return False
        else:
            return True

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelTimeBoardingsAndRoute other):
        if any([self.arrival_time_target > other.arrival_time_target, self.n_boardings > other.n_boardings]):
            return False
        elif all([self.arrival_time_target == other.arrival_time_target, self.n_boardings == other.n_boardings]) \
                and self.movement_duration > other.movement_duration:
            return False
        else:
            return True

    cpdef int dominates_ignoring_dep_time(self, LabelTimeBoardingsAndRoute other):
        if any([self.arrival_time_target > other.arrival_time_target,
                self.n_boardings > other.n_boardings,
                self.first_leg_is_walk > other.first_leg_is_walk]):
            return False
        elif all([self.arrival_time_target == other.arrival_time_target,
                self.n_boardings == other.n_boardings,
                self.first_leg_is_walk == other.first_leg_is_walk]) and self.movement_duration > other.movement_duration:
            return False
        else:
            return True

    cpdef int dominates_ignoring_time(self, LabelTimeBoardingsAndRoute other):
        if any([self.n_boardings > other.n_boardings,
                self.first_leg_is_walk > other.first_leg_is_walk]):
            return False
        elif all([self.n_boardings == other.n_boardings,
                self.first_leg_is_walk == other.first_leg_is_walk]) and self.movement_duration > other.movement_duration:
            return False
        else:
            return True

    cpdef int dominates_ignoring_dep_time_and_n_boardings(self, LabelTimeBoardingsAndRoute other):
        if any([self.arrival_time_target > other.arrival_time_target,
                self.first_leg_is_walk > other.first_leg_is_walk]):
            return False
        elif all([self.arrival_time_target == other.arrival_time_target,
                self.first_leg_is_walk == other.first_leg_is_walk]) and self.movement_duration > other.movement_duration:
            return False
        else:
            return True

    cpdef get_label_with_connection_added(self, connection):
        return LabelTimeBoardingsAndRoute(self.departure_time, self.arrival_time_target,
                                           self.n_boardings, self.movement_duration, self.first_leg_is_walk, connection=connection, previous_label=self)
    cpdef get_copy(self):
        return LabelTimeBoardingsAndRoute(self.departure_time, self.arrival_time_target,
                                           self.n_boardings, self.movement_duration, self.first_leg_is_walk, self.connection, previous_label=self.previous_label)

    cpdef get_copy_with_specified_departure_time(self, departure_time):
        return LabelTimeBoardingsAndRoute(departure_time, self.arrival_time_target,
                                           self.n_boardings, self.movement_duration, self.first_leg_is_walk, self.connection, previous_label=self.previous_label)

    cpdef double duration(self):
        return self.arrival_time_target - self.departure_time

    @staticmethod
    def direct_walk_label(departure_time, walk_duration):
        return LabelTimeBoardingsAndRoute(departure_time, departure_time + walk_duration, 0, True)

    cpdef LabelTimeBoardingsAndRoute get_copy_with_walk_added(self, double walk_duration, object connection):
        return LabelTimeBoardingsAndRoute(self.departure_time - walk_duration,
                                           self.arrival_time_target, self.n_boardings, self.movement_duration+walk_duration, True, connection=connection, previous_label=self)

    def __str__(self):
        return str((self.departure_time, self.arrival_time_target, self.n_boardings, self.movement_duration, self.first_leg_is_walk, self.previous_label, self.connection))

cdef class LabelTimeAndRoute:
    # implement added constraint for cases when two labels are tied:
    # The trip with minimal "movement time" should be chosen = maximizing the waiting time for robustness
    cdef:
        public double departure_time
        public double arrival_time_target
        public int movement_duration
        public bint first_leg_is_walk
        public object previous_label
        public object connection


    def __init__(self, double departure_time, double arrival_time_target,
                 int movement_duration, bint first_leg_is_walk, object connection=None, object previous_label=None):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target
        self.movement_duration = movement_duration
        self.first_leg_is_walk = first_leg_is_walk
        self.previous_label = previous_label
        self.connection = connection

    def __getstate__(self):
        return self.departure_time, self.arrival_time_target, self.movement_duration, self.connection, self.previous_label

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target, self.movement_duration, self.connection, self.previous_label = state

    def _tuple_for_ordering(self):
        return self.departure_time, -self.arrival_time_target, not self.first_leg_is_walk, -self.movement_duration

    def __richcmp__(LabelTimeAndRoute self, LabelTimeAndRoute other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2:  # ==
            return self_tuple == other_tuple
        if op == 3:  # !=
            return self_tuple != other_tuple
        if op == 0:  # less than
            return self_tuple < other_tuple
        elif op == 4:  # greater than
            return self_tuple > other_tuple
        elif op == 1:  # <=
            return self_tuple <= other_tuple
        elif op == 5:  # >=
            return self_tuple >= other_tuple

    cpdef int dominates(self, LabelTimeAndRoute other):
        """
        Compute whether this LabelWithNumberVehicles dominates the other LabelWithNumberVehicles
        Parameters
        ----------
        other: LabelTimeAndRoute
        Returns
        -------
        dominates: bint
            True if this ParetoTuple dominates the other, otherwise False
        """
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if any([(s < o) for s, o in zip(self_tuple[:-1], other_tuple[:-1])]):
            return False
        elif all([(s == o) for s, o in zip(self_tuple[:-1], other_tuple[:-1])]) and self_tuple[-1] < other_tuple[-1]:
            return False
        else:
            return True

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelTimeAndRoute other):
        dominates = (
            self.arrival_time_target <= other.arrival_time_target
        )
        return dominates

    cpdef int dominates_ignoring_dep_time(self, LabelTimeAndRoute other):
        cdef:
            int dominates
        dominates = (
            (self.arrival_time_target <= other.arrival_time_target and
            self.first_leg_is_walk <= other.first_leg_is_walk) or
            (self.arrival_time_target == other.arrival_time_target and
            self.first_leg_is_walk == other.first_leg_is_walk and self.movement_duration <= other.movement_duration)

        )
        return dominates

    cpdef int dominates_ignoring_time(self, LabelTimeAndRoute other):
        cdef:
            int dominates
        dominates = (
            self.movement_duration <= other.movement_duration and
            self.first_leg_is_walk <= other.first_leg_is_walk
        )
        return dominates

    cpdef int dominates_ignoring_dep_time_and_n_boardings(self, LabelTimeAndRoute other):
        cdef:
            int dominates
        dominates = (
            self.arrival_time_target <= other.arrival_time_target and
            self.first_leg_is_walk <= other.first_leg_is_walk
        )
        return dominates

    cpdef get_label_with_connection_added(self, connection):
        return LabelTimeAndRoute(self.departure_time, self.arrival_time_target,
                                           self.movement_duration, self.first_leg_is_walk, connection=connection, previous_label=self)
    cpdef get_copy(self):
        return LabelTimeAndRoute(self.departure_time, self.arrival_time_target,
                                           self.movement_duration, self.first_leg_is_walk, self.connection, previous_label=self.previous_label)

    cpdef get_copy_with_specified_departure_time(self, departure_time):
        return LabelTimeAndRoute(departure_time, self.arrival_time_target,
                                           self.movement_duration, self.first_leg_is_walk, self.connection, previous_label=self.previous_label)

    cpdef double duration(self):
        return self.arrival_time_target - self.departure_time

    @staticmethod
    def direct_walk_label(departure_time, walk_duration):
        return LabelTimeAndRoute(departure_time, departure_time + walk_duration, 0, True)

    cpdef LabelTimeAndRoute get_copy_with_walk_added(self, double walk_duration, object connection):
        return LabelTimeAndRoute(self.departure_time - walk_duration,
                                           self.arrival_time_target, self.movement_duration+walk_duration, True, connection=connection, previous_label=self)

    def __str__(self):
        return str((self.departure_time, self.arrival_time_target, self.movement_duration, self.first_leg_is_walk, self.previous_label, self.connection))


cdef class LabelGeneric:
    # This class is used only for the analysis stage, when the data has been stored in a database
    cdef public int journey_id,  from_stop_I, to_stop_I, n_boardings, movement_duration, journey_duration, \
        in_vehicle_duration, transfer_wait_duration, walking_duration, pre_journey_wait_fp
    cdef public double departure_time, arrival_time_target

    def __init__(self, journey_dict):
        for key in journey_dict:
            setattr(self, key, journey_dict[key])
        # Assert that key attributes are present
        assert hasattr(self, "journey_id")
        assert hasattr(self, "from_stop_I")
        assert hasattr(self, "to_stop_I")

    def __getstate__(self):
        return self.departure_time, self.arrival_time_target, self.movement_duration, self.connection, self.previous_label

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target, self.movement_duration, self.connection, self.previous_label = state

    def _tuple_for_ordering(self):
        return self.departure_time, -self.arrival_time_target, -self.movement_duration

    def __richcmp__(LabelGeneric self, LabelGeneric other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2:  # ==
            return self_tuple == other_tuple
        if op == 3:  # !=
            return self_tuple != other_tuple
        if op == 0:  # less than
            return self_tuple < other_tuple
        elif op == 4:  # greater than
            return self_tuple > other_tuple
        elif op == 1:  # <=
            return self_tuple <= other_tuple
        elif op == 5:  # >=
            return self_tuple >= other_tuple

    cpdef int dominates(self, LabelGeneric other):
        """
        Compute whether this LabelWithNumberVehicles dominates the other LabelWithNumberVehicles
        Parameters
        ----------
        other: LabelGeneric
        Returns
        -------
        dominates: bint
            True if this ParetoTuple dominates the other, otherwise False
        """
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if any([(s < o) for s, o in zip(self_tuple[:-1], other_tuple[:-1])]):
            return False
        elif all([(s == o) for s, o in zip(self_tuple[:-1], other_tuple[:-1])]) and self_tuple[-1] < other_tuple[-1]:
            return False
        else:
            return True

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelGeneric other):
        dominates = (
            self.arrival_time_target <= other.arrival_time_target
        )
        return dominates

    cpdef int dominates_ignoring_dep_time(self, LabelGeneric other):
        cdef:
            int dominates
        dominates = (
            (self.arrival_time_target <= other.arrival_time_target) or
            (self.arrival_time_target == other.arrival_time_target and self.movement_duration <= other.movement_duration)

        )
        return dominates

    cpdef int dominates_ignoring_time(self, LabelGeneric other):
        cdef:
            int dominates
        dominates = (
            self.movement_duration <= other.movement_duration
        )
        return dominates

    cpdef int dominates_ignoring_dep_time_and_n_boardings(self, LabelGeneric other):
        cdef:
            int dominates
        dominates = (
            self.arrival_time_target <= other.arrival_time_target
        )
        return dominates

    cpdef get_label_with_connection_added(self, connection):
        return LabelGeneric(self.departure_time, self.arrival_time_target,
                                           self.movement_duration, connection=connection, previous_label=self)
    cpdef get_copy(self):
        return LabelGeneric(self.departure_time, self.arrival_time_target,
                                           self.movement_duration, self.connection, previous_label=self.previous_label)

    cpdef get_copy_with_specified_departure_time(self, departure_time):
        return LabelGeneric(departure_time, self.arrival_time_target,
                                           self.movement_duration, self.connection, previous_label=self.previous_label)

    cpdef double duration(self):
        return self.arrival_time_target - self.departure_time

    @staticmethod
    def direct_walk_label(departure_time, walk_duration):
        return LabelGeneric(departure_time, departure_time + walk_duration, 0, True)

    cpdef LabelGeneric get_copy_with_walk_added(self, double walk_duration, object connection):
        return LabelGeneric(self.departure_time - walk_duration,
                                           self.arrival_time_target, self.movement_duration+walk_duration, True, connection=connection, previous_label=self)

    def __str__(self):
        return str((self.departure_time, self.arrival_time_target, self.movement_duration))

