from gtfspy import mapviz
from example_import import load_or_import_gtfs
from matplotlib import pyplot as plt

g = load_or_import_gtfs()

ax = mapviz.plot_route_network(g)
ax = mapviz.plot_all_stops(g, ax)

plt.show()
