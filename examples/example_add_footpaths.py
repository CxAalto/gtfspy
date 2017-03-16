import example_import
from gtfspy.gtfs import GTFS

# Note!
# The support for

G = example_import.import_database(verbose=False)
assert(isinstance(G, GTFS))
print(G.))

