from matplotlib import pyplot as plt

import example_import
from gtfspy.route_types import ROUTE_TYPE_TO_COLOR
from gtfspy.routing.helpers import get_transit_connections, get_walk_network
from gtfspy.routing.journey_animator import JourneyAnimator
from gtfspy.routing.journey_path_analyzer import NodeJourneyPathAnalyzer
from gtfspy.routing.multi_objective_pseudo_connection_scan_profiler import MultiObjectivePseudoCSAProfiler

G = example_import.load_or_import_example_gtfs()
tz = G.get_timezone_pytz()

from_stop_name = "Ahkiotie 2 E"
to_stop_name = "Kauppahalli P"
from_stop_I = None
to_stop_I = None
stop_dict = G.stops().to_dict("index")
for stop_I, data in stop_dict.items():
    if data['name'] == from_stop_name:
        from_stop_I = stop_I
    if data['name'] == to_stop_name:
        to_stop_I = stop_I
assert (from_stop_I is not None)
assert (to_stop_I is not None)

ROUTING_START_TIME_UT = G.get_suitable_date_for_daily_extract(ut=True) + 10 * 3600
ROUTING_END_TIME_UT = G.get_suitable_date_for_daily_extract(ut=True) + 14 * 3600

connections = get_transit_connections(G, ROUTING_START_TIME_UT, ROUTING_END_TIME_UT)
walk_network = get_walk_network(G)

mpCSA = MultiObjectivePseudoCSAProfiler(connections, targets=[to_stop_I], walk_network=walk_network,
                                        end_time_ut=ROUTING_END_TIME_UT, transfer_margin=120,
                                        start_time_ut=ROUTING_START_TIME_UT, walk_speed=1.5, verbose=True,
                                        track_vehicle_legs=True, track_time=True, track_route=True)

mpCSA.run()
profiles = mpCSA.stop_profiles

CUTOFF_TIME = 2 * 3600
labels = dict((key, value.get_final_optimal_labels()) for (key, value) in profiles.items())
walk_times = dict((key, value.get_walk_to_target_duration()) for (key, value) in profiles.items())

nra = NodeJourneyPathAnalyzer(labels[from_stop_I], walk_times[from_stop_I], ROUTING_START_TIME_UT,
                              ROUTING_END_TIME_UT - CUTOFF_TIME, from_stop_I)

fig = plt.figure()
ax1 = fig.add_subplot(221)

journey_path_letters, stop_letter_dict = nra.assign_path_letters(nra.journey_boarding_stops)
nra.plot_fastest_temporal_distance_profile(ax=ax1,
                                           timezone=tz,
                                           plot_journeys=True,
                                           journey_letters=journey_path_letters,
                                           format_string="%H:%M:%S")

nra.gtfs = G
ax2 = fig.add_subplot(222, projection="smopy_axes")

for lats, lons, leg_type in nra.get_journey_trajectories():
    ax2.plot(lons, lats, c=ROUTE_TYPE_TO_COLOR[leg_type], zorder=1)

lat, lon = G.get_stop_coordinates(to_stop_I)
ax2.scatter(lon, lat, s=100, c="green", marker="X", zorder=2)
lat, lon = G.get_stop_coordinates(from_stop_I)
ax2.scatter(lon, lat, s=100, c="red", marker="X", zorder=2)
for stop_id, letters in stop_letter_dict.items():
    lat, lon = G.get_stop_coordinates(stop_id)
    ax2.scatter(lon, lat, s=20, c="grey", marker="o", zorder=2)
    text = ax2.text(lon, lat, ",".join(letters), color="m", fontsize=10, va="top", ha="left", zorder=10)

ax2.add_scale_bar()

ax3 = fig.add_subplot(223)
diversity_dict = nra.get_simple_diversities()
ax3.axis('tight')
ax3.axis('off')
the_table = ax3.table(cellText=[[str(round(x, 3))] for x in diversity_dict.values()],
                      rowLabels=list(diversity_dict.keys()),
                      colWidths=[0.5, 0.2],
                      loc='center')
the_table.auto_set_font_size(False)
the_table.set_fontsize(10)

ax4 = fig.add_subplot(224)
ax4 = nra.plot_journey_graph(ax4)

# An animation showing the optimal journey alternatives:
ea = JourneyAnimator(labels[from_stop_I], G)
ani = ea.animation(anim_length_seconds=60, fps=10)

# ani is an instance of matplotlib.animation.FuncAnimation
# ani.save('test_video.mp4')

plt.show()
