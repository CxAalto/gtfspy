from collections import Counter, defaultdict
import sys


class WarningsContainer(object):
    def __init__(self):
        self._warnings_counter = Counter()
        # key: "warning type" string, value: "number of errors" int
        self._warnings_records = defaultdict(list)
        # key: "warning_type" string, value: "list of dataframes etc." providing the details

    def add_warning(self, warning, reason, count=None):
        if count == 0:
            return
        if count is not None:
            self._warnings_counter[warning] += count
        else:
            self._warnings_counter[warning] += 1
        self._warnings_records[warning].append(reason)

    def write_summary(self, output_stream=None):
        if output_stream is None:
            output_stream = sys.stdout
        output_stream.write("The feed produced the following warnings:\n")
        for warning, count in self._warnings_counter.most_common():
            output_stream.write(warning + ": " + str(count) + "\n")

    def write_details(self, output_stream=None):
        if output_stream is None:
            output_stream = sys.stdout
        output_stream.write("The feed produced the following warnings (with details):\n")
        for warning, count in self._warnings_counter.most_common():
            output_stream.write(warning + ": " + str(count) + "\n")
            for reason in self._warnings_records[warning]:
                output_stream.write(str(reason) + "\n")

    def get_warning_counter(self):
        """
        Returns
        -------
        counter: collections.Counter
        """
        return self._warnings_counter

    def get_warnings_by_query_rows(self):
        """
        Returns
        -------
        warnings_record: defaultdict(list)
            maps each row to a list of warnings
        """
        return self._warnings_records

    def clear(self):
        self._warnings_counter.clear()
        self._warnings_records.clear()
