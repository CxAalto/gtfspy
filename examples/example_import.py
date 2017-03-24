import os

from gtfspy import import_gtfs
from gtfspy import gtfs
from gtfspy import osm_transfers


def import_example(verbose=False):
    imported_database_path = "test_db_kuopio.sqlite"
    if not os.path.exists(imported_database_path):  # reimport only if the imported database does not already exist
        print("Importing gtfs zip file")
        import_gtfs.import_gtfs(["data/gtfs_kuopio_finland.zip"],  # input: list of GTFS zip files (or directories)
                                imported_database_path,  # output: where to create the new sqlite3 database
                                print_progress=verbose,  # whether to print progress when importing data
                                location_name="Kuopio")

        # Not this is an optional step, which is not necessary for many things.
        print("Computing walking paths using OSM")

        osm_path = "data/kuopio_extract_mapzen_2017_03_15.osm.pbf"

        # when using with the Kuopio test data set,
        # this should raise a warning due to no nearby OSM nodes for one of the stops.
        osm_transfers.add_walk_distances_to_db_python(imported_database_path, osm_path)

        print("Note: for large cities we have also a faster option for computing footpaths that uses Java.)")
        dir_path = os.path.dirname(os.path.realpath(__file__))
        java_path = os.path.join(dir_path, "../java_routing/")
        print("Please see the contents of " + java_path + " for more details.")

    # Now you can access the imported database using a GTFS-object as an interface:
    G = gtfs.GTFS(imported_database_path)

    if verbose:
        print("Location name:" + G.get_location_name())  # should print Kuopio
        print("Time span of the data in unixtime: " + str(G.get_conservative_gtfs_time_span_in_ut()))
        # prints the time span in unix time
    return G


if __name__ == "__main__":
    import_example(verbose=True)
