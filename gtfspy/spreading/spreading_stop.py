class SpreadingStop:
    def __init__(self, stop_I, min_transfer_time):
        self.stop_I = stop_I
        self.min_transfer_time = min_transfer_time
        self.visit_events = []

    def get_min_visit_time(self):
        """
        Get the earliest visit time of the stop.
        """
        if not self.visit_events:
            return float("inf")
        else:
            return min(self.visit_events, key=lambda event: event.arr_time_ut).arr_time_ut

    def get_min_event(self):
        if len(self.visit_events) == 0:
            return None
        else:
            return min(self.visit_events, key=lambda event: event.arr_time_ut)

    def visit(self, event):
        """
        Visit the stop if it has not been visited already by an event with
        earlier arr_time_ut (or with other trip that does not require a transfer)

        Parameters
        ----------
        event : Event
            an instance of the Event (namedtuple)

        Returns
        -------
        visited : bool
            if visit is stored, returns True, otherwise False
        """
        to_visit = False
        if event.arr_time_ut <= self.min_transfer_time + self.get_min_visit_time():
            to_visit = True
        else:
            for ve in self.visit_events:
                if (event.trip_I == ve.trip_I) and event.arr_time_ut < ve.arr_time_ut:
                    to_visit = True

        if to_visit:
            self.visit_events.append(event)
            min_time = self.get_min_visit_time()
            # remove any visits that are 'too old'
            self.visit_events = [
                v for v in self.visit_events if v.arr_time_ut <= min_time + self.min_transfer_time
            ]
        return to_visit

    def has_been_visited(self):
        return len(self.visit_events) > 0

    def can_infect(self, event):
        """
        Whether the spreading stop can infect using this event.
        """
        if event.from_stop_I != self.stop_I:
            return False

        if not self.has_been_visited():
            return False
        else:
            time_sep = event.dep_time_ut - self.get_min_visit_time()
            # if the gap between the earliest visit_time and current time is
            # smaller than the min. transfer time, the stop can pass the spreading
            # forward
            if (time_sep >= self.min_transfer_time) or (event.trip_I == -1 and time_sep >= 0):
                return True
            else:
                for visit in self.visit_events:
                    # if no transfer, please hop-on
                    if (event.trip_I == visit.trip_I) and (time_sep >= 0):
                        return True
            return False
