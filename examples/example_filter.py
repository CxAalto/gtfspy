import datetime
import os

from example_import import load_or_import_example_gtfs
from gtfspy.gtfs import GTFS
from gtfspy.filter import FilterExtract

G = load_or_import_example_gtfs()
assert isinstance(G, GTFS)

filtered_database_path = "test_db_kuopio.week.sqlite"
# remove the old file, if exists
if os.path.exists(filtered_database_path):
    os.remove(filtered_database_path)

# filter by time and 3 kilometers from the city center
week_start = G.get_weekly_extract_start_date()
week_end = week_start + datetime.timedelta(days=7)
fe = FilterExtract(G, filtered_database_path, start_date=week_start, end_date=week_end,
                   buffer_lat=62.8930796, buffer_lon=27.6671316, buffer_distance=3)

fe.create_filtered_copy()
assert (os.path.exists(filtered_database_path))

G = GTFS(filtered_database_path)

# visualize the routes of the filtered database
from gtfspy import mapviz
from matplotlib import pyplot as plt
mapviz.plot_route_network_from_gtfs(G)
plt.show()