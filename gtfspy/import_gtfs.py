# reload(sys)
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from gtfspy.import_loaders import (
    AgencyLoader,
    CalendarDatesLoader,
    CalendarLoader,
    DayLoader,
    DayTripsMaterializer,
    FeedInfoLoader,
    FrequenciesLoader,
    TripLoader,
    MetadataLoader,
    RouteLoader,
    ShapeLoader,
    StopDistancesLoader,
    StopLoader,
    StopTimesLoader,
    TransfersLoader,
)
from gtfspy.import_loaders.table_loader import ignore_tables, decode_six

"""
Importing GTFS into a sqlite database.

Entry point: see main part at the bottom and/or the import_gtfs function.
"""

import re
import sqlite3
import time
from six import string_types

from gtfspy import stats
from gtfspy import util
from gtfspy.gtfs import GTFS


Loaders = [
    AgencyLoader,  # deps: -
    RouteLoader,  # deps: Agency
    MetadataLoader,  # deps: -
    CalendarLoader,  # deps: -
    CalendarDatesLoader,  # deps: Calendar
    ShapeLoader,  # deps: -
    FeedInfoLoader,  # deps: -
    StopLoader,  # deps: -
    TransfersLoader,  # deps: Stop
    StopDistancesLoader,  # deps: (pi: Stop)
    TripLoader,  # deps: Route, Calendar, (Shape)             | (pi2: StopTimes)
    StopTimesLoader,  # deps: Stop, Trip                           |                  |(v: Trip, Day)
    FrequenciesLoader,  # deps: Trip (pi: Trip, StopTimes)           |
    DayLoader,  # deps: (pi: Calendar, CalendarDates, Trip)  |
    DayTripsMaterializer,  # deps:                                      | (pi2: Day)
]
postprocessors = [
    # validate_day_start_ut,
]


def import_gtfs(
    gtfs_sources,
    output,
    preserve_connection=False,
    print_progress=True,
    location_name=None,
    **kwargs
):
    """Import a GTFS database

    gtfs_sources: str, dict, list
        Paths to the gtfs zip file or to the directory containing the GTFS data.
        Alternatively, a dict can be provide that maps gtfs filenames
        (like 'stops.txt' and 'agencies.txt') to their string presentations.

    output: str or sqlite3.Connection
        path to the new database to be created, or an existing
        sqlite3 connection
    preserve_connection: bool, optional
        Whether to close the connection in the end, or not.
    print_progress: bool, optional
        Whether to print progress output
    location_name: str, optional
        set the location of this database
    """
    if isinstance(output, sqlite3.Connection):
        conn = output
    else:
        # if os.path.isfile(output):
        #  raise RuntimeError('File already exists')
        conn = sqlite3.connect(output)
    if not isinstance(gtfs_sources, list):
        gtfs_sources = [gtfs_sources]
    cur = conn.cursor()
    time_import_start = time.time()

    # These are a bit unsafe, but make importing much faster,
    # especially on scratch.
    cur.execute("PRAGMA page_size = 4096;")
    cur.execute("PRAGMA mmap_size = 1073741824;")
    cur.execute("PRAGMA cache_size = -2000000;")
    cur.execute("PRAGMA temp_store=2;")
    # Changes of isolation level are python3.6 workarounds -
    # eventually will probably be fixed and this can be removed.
    conn.isolation_level = None  # change to autocommit mode (former default)
    cur.execute("PRAGMA journal_mode = OFF;")
    # cur.execute('PRAGMA journal_mode = WAL;')
    cur.execute("PRAGMA synchronous = OFF;")
    conn.isolation_level = ""  # change back to python default.
    # end python3.6 workaround

    # Do the actual importing.
    loaders = [L(gtfssource=gtfs_sources, print_progress=print_progress, **kwargs) for L in Loaders]

    for loader in loaders:
        loader.assert_exists_if_required()

    # Do initial import.  This consists of making tables, raw insert
    # of the CSVs, and then indexing.

    for loader in loaders:
        loader.import_(conn)

    # Do any operations that require all tables present.
    for Loader in loaders:
        Loader.post_import_round2(conn)

    # Make any views
    for Loader in loaders:
        Loader.make_views(conn)

    # Make any views
    for F in postprocessors:
        F(conn)

    # Set up same basic metadata.
    from gtfspy import gtfs as mod_gtfs

    G = mod_gtfs.GTFS(output)
    G.meta["gen_time_ut"] = time.time()
    G.meta["gen_time"] = time.ctime()
    G.meta["import_seconds"] = time.time() - time_import_start
    G.meta["download_date"] = ""
    G.meta["location_name"] = ""
    G.meta["n_gtfs_sources"] = len(gtfs_sources)

    # Extract things from GTFS
    download_date_strs = []
    for i, source in enumerate(gtfs_sources):
        if len(gtfs_sources) == 1:
            prefix = ""
        else:
            prefix = "feed_" + str(i) + "_"
        if isinstance(source, string_types):
            G.meta[prefix + "original_gtfs"] = decode_six(source) if source else None
            # Extract GTFS date.  Last date pattern in filename.
            filename_date_list = re.findall(r"\d{4}-\d{2}-\d{2}", source)
            if filename_date_list:
                date_str = filename_date_list[-1]
                G.meta[prefix + "download_date"] = date_str
                download_date_strs.append(date_str)
            if location_name:
                G.meta["location_name"] = location_name
            else:
                location_name_list = re.findall(r"/([^/]+)/\d{4}-\d{2}-\d{2}", source)
                if location_name_list:
                    G.meta[prefix + "location_name"] = location_name_list[-1]
                else:
                    try:
                        G.meta[prefix + "location_name"] = source.split("/")[-4]
                    except:
                        G.meta[prefix + "location_name"] = source

    if G.meta["download_date"] == "":
        unique_download_dates = list(set(download_date_strs))
        if len(unique_download_dates) == 1:
            G.meta["download_date"] = unique_download_dates[0]

    G.meta["timezone"] = cur.execute("SELECT timezone FROM agencies LIMIT 1").fetchone()[0]
    stats.update_stats(G)
    del G

    if print_progress:
        print("Vacuuming...")
    # Next 3 lines are python 3.6 work-arounds again.
    conn.isolation_level = None  # former default of autocommit mode
    cur.execute("VACUUM;")
    conn.isolation_level = ""  # back to python default
    # end python3.6 workaround
    if print_progress:
        print("Analyzing...")
    cur.execute("ANALYZE")
    if not (preserve_connection is True):
        conn.close()


def validate_day_start_ut(conn):
    """This validates the day_start_ut of the days table."""
    G = GTFS(conn)
    cur = conn.execute("SELECT date, day_start_ut FROM days")
    for date, day_start_ut in cur:
        # print date, day_start_ut
        assert day_start_ut == G.get_day_start_ut(date)


def main_make_views(gtfs_fname):
    """Re-create all views.
    """
    print("creating views")
    conn = GTFS(fname_or_conn=gtfs_fname).conn
    for L in Loaders:
        L(None).make_views(conn)
    conn.commit()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="""
    Import GTFS files.  Imports gtfs.  There are two subcommands.  The
    'import' subcommand converts from a GTFS directory or zipfile to a
    sqlite database.  Both must be specified on the command line.  The
    import-auto subcommand is older, and uses the logic in code/db.py to
    automatically find databases and output files (in scratch/gtfs and
    scratch/db) based on the shortname given on the command line.  This
    should probably not be used much anymore, instead automate that before
    calling this program."""
    )
    subparsers = parser.add_subparsers(dest="cmd")
    # parsing import
    parser_import = subparsers.add_parser("import", help="Direct import GTFS->sqlite")
    parser_import.add_argument("gtfs", help="Input GTFS filename (dir or .zip)")
    parser_import.add_argument("output", help="Output .sqlite filename (must end in .sqlite)")
    parser.add_argument("--fast", action="store_true", help="Skip stop_times and shapes tables.")

    # parsing import-auto
    parser_importauto = subparsers.add_parser(
        "import-auto", help="Automatic GTFS import from files"
    )
    parser_importauto.add_argument("gtfsname", help="Input GTFS filename")

    # parsing import-multiple
    parser_import_multiple = subparsers.add_parser(
        "import-multiple", help="GTFS import from multiple zip-files"
    )
    parser_import_multiple.add_argument(
        "zipfiles",
        metavar="zipfiles",
        type=str,
        nargs=argparse.ONE_OR_MORE,
        help="zipfiles for the import",
    )
    parser_import_multiple.add_argument(
        "output", help="Output .sqlite filename (must end in .sqlite)"
    )

    # parsing import-list

    # Parsing copy
    parser_copy = subparsers.add_parser("copy", help="Copy database")
    parser_copy.add_argument("source", help="Input GTFS .sqlite")
    parser_copy.add_argument("dest", help="Output GTFS .sqlite")
    parser_copy.add_argument("--start", help="Start copy time (inclusive)")
    parser_copy.add_argument("--end", help="Start copy time (exclusive)")

    # Parsing copy
    parser_copy = subparsers.add_parser("make-views", help="Re-create views")
    parser_copy.add_argument("gtfs", help="Input GTFS .sqlite")

    # make-weekly-download
    parser_copy = subparsers.add_parser("make-weekly")
    parser_copy.add_argument("source", help="Input GTFS .sqlite")
    parser_copy.add_argument("dest", help="Output GTFS .sqlite")

    parser_copy = subparsers.add_parser("make-daily")
    parser_copy.add_argument("source", help="Input GTFS .sqlite")
    parser_copy.add_argument("dest", help="Output GTFS .sqlite")

    # Export stop distances
    parser_copy = subparsers.add_parser("export-stop-distances")
    parser_copy.add_argument("gtfs", help="Input GTFS .sqlite")
    parser_copy.add_argument("output", help="Output for .txt file")

    # Custom stuff
    parser_copy = subparsers.add_parser("custom")
    parser_copy.add_argument("gtfs", help="Input GTFS .sqlite")

    args = parser.parse_args()
    if args.fast:
        ignore_tables.update(("stop_times", "shapes"))
    # if the first argument is import, import a GTFS directory to a .sqlite database.
    # Both directory and
    if args.cmd == "import":
        gtfs = args.gtfs
        output = args.output
        # This context manager makes a tmpfile for import.  If there
        # is corruption during import, it won't leave a incomplete or
        # corrupt file where it will be noticed.
        with util.create_file(output, tmpdir=True, keepext=True) as tmpfile:
            import_gtfs(gtfs, output=tmpfile)
    elif args.cmd == "import-multiple":
        zipfiles = args.zipfiles
        output = args.output
        print("loaders")
        with util.create_file(output, tmpdir=True, keepext=True) as tmpfile:
            import_gtfs(zipfiles, output=tmpfile)
    elif args.cmd == "make-views":
        main_make_views(args.gtfs)
    # This is now implemented in gtfs.py, please remove the commented code
    # if no one has touched this in a while.:
    #
    # elif args.cmd == 'make-weekly':
    #     G = GTFS(args.source)
    #     download_date = G.meta['download_date']
    #     d = datetime.strptime(download_date, '%Y-%m-%d').date()
    #     date_start = d + timedelta(7-d.isoweekday()+1)      # inclusive
    #     date_end   = d + timedelta(7-d.isoweekday()+1 + 7)  # exclusive
    #     G.copy_and_filter(args.dest, start_date=date_start, end_date=date_end)
    # elif args.cmd == 'make-daily':
    #     G = GTFS(args.source)
    #     download_date = G.meta['download_date']
    #     d = datetime.strptime(download_date, '%Y-%m-%d').date()
    #     date_start = d + timedelta(7-d.isoweekday()+1)      # inclusive
    #     date_end   = d + timedelta(7-d.isoweekday()+1 + 1)  # exclusive
    #     G.copy_and_filter(args.dest, start_date=date_start, end_date=date_end)
    elif args.cmd == "export-stop-distances":
        conn = sqlite3.connect(args.gtfs)
        L = StopDistancesLoader(conn)
        L.export_stop_distances(conn, open(args.output, "w"))
    elif args.cmd == "custom":
        pass
        # This is designed for just testing things.  This code should
        # always be commented out in the VCS.
        # conn = sqlite3.connect(args.gtfs)
        # L = StopDistancesLoader(conn)
        # L.post_import(None)
        # L.export_stop_distances(conn, open('sd.txt', 'w'))
    else:
        print("Unrecognized command: %s" % args.cmd)
        exit(1)


if __name__ == "__main__":
    main()
