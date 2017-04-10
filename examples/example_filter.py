import datetime
import os

from example_import import load_or_import_example_gtfs
from gtfspy.gtfs import GTFS
from gtfspy.filter import FilterExtract

G = load_or_import_example_gtfs()
assert isinstance(G, GTFS)

filtered_database_path = "test_db_kuopio.week.sqlite"
if os.path.exists(filtered_database_path):
    os.remove(filtered_database_path)

week_start = G.get_weekly_extract_start_date()
week_end = week_start + datetime.timedelta(days=7)
fe = FilterExtract(G, filtered_database_path, start_date=week_start, end_date=week_end)

fe.create_filtered_copy()
assert (os.path.exists(filtered_database_path))
