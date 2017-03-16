import os

from gtfspy import import_gtfs
from gtfspy import gtfs


def import_example_database(verbose=True):
    """
    This importing function is used also by other examples, and is provided as a function.
    """
    imported_database_path = "test_db.sqlite"
    if not os.path.exists(imported_database_path):   # reimport only if the imported database does not already exist
        import_gtfs.import_gtfs(["data/gtfs_kuopio_finland.zip"],  # input: list of GTFS zip files (or directories)
                                imported_database_path,
                                print_progress=verbose,
                                location_name="Kuopio")  # output: path to the new database or a sqlite3 database connection object

    # Access the imported database using a GTFS-object as an interface:
    G = gtfs.GTFS(imported_database_path)

    if verbose:
        print(G.get_location_name())  # should print Kuopio
        print(G.get_conservative_gtfs_time_span_in_ut())  # prints the time span in unix time

        table = G.get_straight_line_transfer_distances()
        print(table[:10])  # print the first 10 stop pairs
    return G


if __name__ == "__main__":
    import_example_database()
