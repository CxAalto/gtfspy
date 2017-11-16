import io
from unittest import TestCase

from gtfspy.warnings_container import WarningsContainer


class TestWarningsContainer(TestCase):

    def test_summary_print(self):
        wc = WarningsContainer()
        wc.add_warning("DUMMY_WARNING", ["dummy1", "dummy2"], 2)

        f = io.StringIO("")
        wc.write_summary(output_stream=f)
        f.seek(0)
        assert len(f.readlines()) == len(wc.get_warning_counter().keys()) + 1

    def test_details_print(self):
        wc = WarningsContainer()
        wc.add_warning("DUMMY_WARNING", ["dummy1", "dummy2"], 2)

        f = io.StringIO("")
        wc.write_details(output_stream=f)
        f.seek(0)
        assert len(f.readlines()) > len(wc.get_warning_counter().keys()) + 1