import functools
import os

from example_import import load_or_import_example_gtfs
from matplotlib import pyplot as plt
from gtfspy.gtfs import GTFS

G = load_or_import_example_gtfs()

daily_trip_counts = G.get_trip_counts_per_day()
f, ax = plt.subplots()

datetimes = [date.to_pydatetime() for date in daily_trip_counts['date']]
trip_counts = daily_trip_counts['trip_counts']

ax.bar(datetimes, trip_counts)
ax.axvline(G.meta['download_date'], color="red")
threshold = 0.96
ax.axhline(trip_counts.max() * threshold, color="red")
ax.axvline(G.get_weekly_extract_start_date(weekdays_at_least_of_max=threshold), color="yellow")

weekly_db_path = "test_db_kuopio.week.sqlite"
if os.path.exists(weekly_db_path):
    G = GTFS(weekly_db_path)
    f, ax = plt.subplots()
    daily_trip_counts = G.get_trip_counts_per_day()
    datetimes = [date.to_pydatetime() for date in daily_trip_counts['date']]
    trip_counts = daily_trip_counts['trip_counts']
    ax.bar(datetimes, trip_counts)

    events = list(G.generate_routable_transit_events(0, G.get_approximate_schedule_time_span_in_ut()[0]))
    min_ut = float('inf')
    for e in events:
        min_ut = min(e.dep_time_ut, min_ut)

    print(G.get_approximate_schedule_time_span_in_ut())

plt.show()



