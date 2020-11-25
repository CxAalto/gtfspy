from gtfspy import mapviz
from example_import import load_or_import_example_gtfs
from matplotlib import pyplot as plt

g = load_or_import_example_gtfs()
# g is now a gtfspy.gtfs.GTFS object

# Plot the route network and all stops to the same axes
ax = mapviz.plot_route_network_from_gtfs(g, scalebar=True)
mapviz.plot_all_stops(g, ax)

# Plot also a thumbnail figure highlighting the central areas:
# ax_thumbnail = mapviz.plot_route_network_thumbnail(g)

# ax_thumbnail.figure.savefig("test_thumbnail.jpg")

plt.show()
