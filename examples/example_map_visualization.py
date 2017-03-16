from gtfspy import mapviz
from example_import import import_example_database
from matplotlib import pyplot as plt

g = import_example_database()

ax = mapviz.plot_route_network(g)
ax = mapviz.plot_all_stops(g, ax)

plt.show()
