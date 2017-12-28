from gtfspy.routing.node_profile_analyzer_time_and_veh_legs import NodeProfileAnalyzerTimeAndVehLegs
from gtfspy.routing.helpers import get_transit_connections, get_walk_network
from gtfspy.routing.multi_objective_pseudo_connection_scan_profiler import MultiObjectivePseudoCSAProfiler
from matplotlib import pyplot as plt
from matplotlib import rc
import example_import

G = example_import.load_or_import_example_gtfs()

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
assert(from_stop_I is not None)
assert(to_stop_I is not None)




ROUTING_START_TIME_UT = G.get_suitable_date_for_daily_extract(ut=True) + 10 * 3600
ROUTING_END_TIME_UT = G.get_suitable_date_for_daily_extract(ut=True) + 14 * 3600

connections = get_transit_connections(G, ROUTING_START_TIME_UT, ROUTING_END_TIME_UT)
walk_network = get_walk_network(G)


mpCSA = MultiObjectivePseudoCSAProfiler(connections, targets=[to_stop_I], walk_network=walk_network,
                                        end_time_ut=ROUTING_END_TIME_UT, transfer_margin=120,
                                        start_time_ut=ROUTING_START_TIME_UT, walk_speed=1.5, verbose=True,
                                        track_vehicle_legs=True, track_time=True)

mpCSA.run()
profiles = mpCSA.stop_profiles

stop_profile = profiles[from_stop_I]
CUTOFF_TIME = 2 * 3600
analyzer = NodeProfileAnalyzerTimeAndVehLegs.from_profile(stop_profile, ROUTING_START_TIME_UT, ROUTING_END_TIME_UT - CUTOFF_TIME)

stop_dict = G.stops().to_dict("index")
print("Origin: ", stop_dict[from_stop_I])
print("Destination: ", stop_dict[to_stop_I])

print("Minimum temporal distance: ", analyzer.min_temporal_distance() / 60., " minutes")
print("Mean temporal distance: ", analyzer.mean_temporal_distance() / 60., " minutes")
print("Maximum temporal distance: ", analyzer.max_temporal_distance() / 60., " minutes")

timezone_pytz = G.get_timezone_pytz()
print("Plotting...")

# use tex in plotting
rc("text", usetex=True)
fig1 = analyzer.plot_new_transfer_temporal_distance_profile(timezone=timezone_pytz,
                                                            format_string="%H:%M")
fig2 = analyzer.plot_temporal_distance_pdf_horizontal(use_minutes=True)
print("Showing...")
plt.show()
