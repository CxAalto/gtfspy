class ProfileBlock:
    def __init__(self, start_time, end_time, distance_start, distance_end, **extra_properties):
        self.start_time = start_time
        self.end_time = end_time
        assert self.start_time < self.end_time
        self.distance_start = distance_start
        self.distance_end = distance_end
        self.extra_properties = extra_properties

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

    def is_flat(self):
        return self.distance_start == self.distance_end

    def interpolate(self, time):
        p = (time - self.start_time) / (self.end_time - self.start_time)
        return (1 - p) * self.distance_start + p * self.distance_end

    def __getitem__(self, extra_property_name):
        return self.extra_properties[extra_property_name]

    def __str__(self):
        parts = []
        parts.append(self.start_time)
        parts.append(self.end_time)
        parts.append(self.distance_start)
        parts.append(self.distance_end)
        parts.append(self.extra_properties)
        return str(parts)
