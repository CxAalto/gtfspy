from example_import import load_or_import_example_gtfs
from gtfspy import networks
from gtfspy import route_types

g = load_or_import_example_gtfs()
day_start_ut = g.get_weekly_extract_start_date(ut=True)

start_ut = day_start_ut + 7 * 3600
end_ut = day_start_ut + 8 * 3600


# get elementary bus events (connections) taking place within a given time interval:
all_events = networks.temporal_network(g,
                                       start_time_ut=start_ut,
                                       end_time_ut=end_ut
                                       )
print("Number of elementary PT events during rush hour in Kuopio: ", len(all_events))

# get  elementary bus events (connections) taking place within a given time interval:
tram_events = networks.temporal_network(g,
                                        start_time_ut=start_ut,
                                        end_time_ut=end_ut,
                                        route_type=route_types.TRAM
                                        )
assert(len(tram_events) == 0) # there should be no trams in our example city (Kuopio, Finland)

# construct a networkx graph
print("\nConstructing a combined stop_to_stop_network")

graph = networks.combined_stop_to_stop_transit_network(g, start_time_ut=start_ut, end_time_ut=end_ut)
print("Number of edges: ", len(graph.edges()))
print("Number of nodes: ", len(graph.nodes()))
print("Example edge: ", list(graph.edges(data=True))[0])
print("Example node: ", list(graph.nodes(data=True))[0])


#################################################
# See also other functions in gtfspy.networks ! #
#################################################

