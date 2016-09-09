import time
import os
import shutil
import logging
import sqlite3
import datetime

import pandas as pd

from gtfspy import util
from gtfspy.util import wgs84_distance
from gtfspy import stats
from gtfspy import gtfs


def filter_extract(gtfs,
                    copy_db_path,
                    buffer_distance=None,
                    buffer_lat=None,
                    buffer_lon=None,
                    update_metadata=True,
                    start_date=None,
                    end_date=None,
                    agency_ids_to_preserve=None,
                    agency_distance=None):


    """
    Copy a database, and then based on various filters.
    Only copy_and_filter method is provided as of now because we do not want to take the risk of
    losing any data of the original databases.

    copy_db_path : str
        path to another database database
    update_metadata : boolean, optional
        whether to update metadata of the feed, defaulting to true
        (this option is mainly available for testing purposes)
    start_date : unicode, or datetime.datetime
        filter out all data taking place before end_date (the start_time_ut of the end date)
        Date format "YYYY-MM-DD"
        (end_date_ut is not included after filtering)
    end_date : unicode, or datetime.datetime
        Filter out all data taking place after end_date
        The end_date is not included after filtering.
    agency_ids_to_preserve : iterable
        List of agency_ids to retain (str) (e.g. 'HSL' for Helsinki)
        Only routes by the listed agencies are then considered
    agency_distance : float
        Only evaluated in combination with agency filter.
        Distance (in km) to the other near-by stops that should be included in addition to
        the ones defined by the agencies.
        All vehicle trips going through at least two such stops would then be included in the
        export. Note that this should not be a recursive thing.
        Or should it be? :)
    buffer_lat : float
        Latitude of the buffer zone center
    buffer_lon : float
        Longitude of the buffer zone center
    buffer_distance : float
        Distance from the buffer zone center (in meters)

    Returns
    -------
    None
    """

    if agency_distance is not None:
        raise NotImplementedError

    this_db_path = gtfs.get_main_database_path()
    assert os.path.exists(this_db_path), "Copying of in-memory databases is not supported"
    assert os.path.exists(os.path.dirname(os.path.abspath(copy_db_path))), \
        "the directory where the copied database will reside should exist beforehand"
    assert not os.path.exists(copy_db_path), "the resulting database exists already: %s" % copy_db_path

    # this with statement
    # is used to ensure that no corrupted/uncompleted files get created in case of problems
    with util.create_file(copy_db_path) as tempfile:
        logging.info("copying database")
        shutil.copy(this_db_path, tempfile)
        copy_db_conn = sqlite3.connect(tempfile)
        assert isinstance(copy_db_conn, sqlite3.Connection)

        _filter_by_start_and_end_date(gtfs, copy_db_conn, start_date, end_date)
        _filter_by_calendar(copy_db_conn, start_date, end_date)
        _filter_by_agency(copy_db_conn, agency_ids_to_preserve)
        _filter_by_area(copy_db_conn, buffer_lat, buffer_lon, buffer_distance)
        _update_metadata(copy_db_conn, gtfs, update_metadata, this_db_path)

    return

def _filter_by_start_and_end_date(gtfs, copy_db_conn, start_date, end_date):
    """
    Removes rows from the sqlite database copy that are out of the time span defined by start_date and end_date
    :param gtfs: GTFS object
    :param copy_db_conn: sqlite database connection
    :param start_date:
    :param end_date:
    :return:
    """
    # filter by start_time_ut and end_date_ut:
    if (start_date is not None) and (end_date is not None):
        logging.info("Filtering based on agency_ids")
        start_date_ut = gtfs.get_day_start_ut(start_date)
        end_date_ut = gtfs.get_day_start_ut(end_date)
        # negated from import_gtfs
        table_to_remove_map = {
            "calendar": ("WHERE NOT ("
                         "date({start_ut}, 'unixepoch', 'localtime') < end_date "
                         "AND "
                         "start_date < date({end_ut}, 'unixepoch', 'localtime')"
                         ");"),
            "calendar_dates": "WHERE NOT ("
                              "date({start_ut}, 'unixepoch', 'localtime') <= date "
                              "AND "
                              "date < date({end_ut}, 'unixepoch', 'localtime')"
                              ")",
            "day_trips2": 'WHERE NOT ('
                          '{start_ut} < end_time_ut '
                          'AND '
                          'start_time_ut < {end_ut}'
                          ')',
            "days": "WHERE NOT ("
                    "{start_ut} <= day_start_ut "
                    "AND "
                    "day_start_ut < {end_ut}"
                    ")"
        }
        # remove the 'source' entries from tables
        for table, query_template in table_to_remove_map.iteritems():
            param_dict = {"start_ut": str(start_date_ut),
                          "end_ut": str(end_date_ut)}
            query = "DELETE FROM " + table + " " + \
                    query_template.format(**param_dict)
            copy_db_conn.execute(query)
    return

def _filter_by_calendar(copy_db_conn, start_date, end_date):
    """
    update calendar table's services
    :param copy_db_conn:
    :param start_date:
    :param end_date:
    :return:
    """

    if (start_date is not None) and (end_date is not None):
        if isinstance(start_date, (datetime.datetime, datetime.date)):
            start_date = start_date.strftime("%Y-%m-%d")
        if not isinstance(end_date, (datetime.datetime, datetime.date)):
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        end_date_to_include = end_date - datetime.timedelta(days=1)
        end_date_to_include_str = end_date_to_include.strftime("%Y-%m-%d")

        start_date_query = "UPDATE calendar " \
                           "SET start_date='{start_date}' " \
                           "WHERE start_date<'{start_date}' ".format(start_date=start_date)
        copy_db_conn.execute(start_date_query)

        end_date_query = "UPDATE calendar " \
                         "SET end_date='{end_date_to_include}' " \
                         "WHERE end_date>'{end_date_to_include}' " \
            .format(end_date_to_include=end_date_to_include_str)
        copy_db_conn.execute(end_date_query)

        # then recursively delete further data:
        copy_db_conn.execute('DELETE FROM trips WHERE '
                             'trip_I NOT IN (SELECT trip_I FROM days)')
        copy_db_conn.execute('DELETE FROM shapes WHERE '
                             'shape_id NOT IN (SELECT shape_id FROM trips)')
        copy_db_conn.execute('DELETE FROM stop_times WHERE '
                             'trip_I NOT IN (SELECT trip_I FROM trips)')
        copy_db_conn.execute('DELETE FROM stops WHERE '
                             'stop_I NOT IN (SELECT stop_I FROM stop_times)')
        copy_db_conn.execute('DELETE FROM stops_rtree WHERE '
                             'stop_I NOT IN (SELECT stop_I FROM stops)')
        copy_db_conn.execute('DELETE FROM stop_distances WHERE '
                             '   from_stop_I NOT IN (SELECT stop_I FROM stops) '
                             'OR to_stop_I   NOT IN (SELECT stop_I FROM stops)')
        copy_db_conn.execute('DELETE FROM routes WHERE '
                             'route_I NOT IN (SELECT route_I FROM trips)')
        copy_db_conn.execute('DELETE FROM agencies WHERE '
                             'agency_I NOT IN (SELECT agency_I FROM routes)')
        copy_db_conn.commit()
    return

def _filter_by_agency(copy_db_conn, agency_ids_to_preserve):
    """
    filter by agency ids
    :param copy_db_conn:
    :param agency_ids_to_preserve:
    :return:
    """
    if agency_ids_to_preserve is not None:
        logging.info("Filtering based on agency_ids")
        agency_ids_to_preserve = list(agency_ids_to_preserve)
        agencies = pd.read_sql("SELECT * FROM agencies", copy_db_conn)
        agencies_to_remove = []
        for idx, row in agencies.iterrows():
            if row['agency_id'] not in agency_ids_to_preserve:
                agencies_to_remove.append(row['agency_id'])
        for agency_id in agencies_to_remove:
            copy_db_conn.execute('DELETE FROM agencies WHERE agency_id=?', (agency_id,))
        # and remove recursively related to the agencies:
        copy_db_conn.execute('DELETE FROM routes WHERE '
                             'agency_I NOT IN (SELECT agency_I FROM agencies)')
        copy_db_conn.execute('DELETE FROM trips WHERE '
                             'route_I NOT IN (SELECT route_I FROM routes)')
        copy_db_conn.execute('DELETE FROM calendar WHERE '
                             'service_I NOT IN (SELECT service_I FROM trips)')
        copy_db_conn.execute('DELETE FROM calendar_dates WHERE '
                             'service_I NOT IN (SELECT service_I FROM trips)')
        copy_db_conn.execute('DELETE FROM days WHERE '
                             'trip_I NOT IN (SELECT trip_I FROM trips)')
        copy_db_conn.execute('DELETE FROM stop_times WHERE '
                             'trip_I NOT IN (SELECT trip_I FROM trips)')
        copy_db_conn.execute('DELETE FROM stop_times WHERE '
                             'trip_I NOT IN (SELECT trip_I FROM trips)')
        copy_db_conn.execute('DELETE FROM shapes WHERE '
                             'shape_id NOT IN (SELECT shape_id FROM trips)')
        copy_db_conn.execute('DELETE FROM day_trips2 WHERE '
                             'trip_I NOT IN (SELECT trip_I FROM trips)')
        copy_db_conn.commit()
    return

def _filter_by_area(copy_db_conn, buffer_lat, buffer_lon, buffer_distance):
    """
    filter by boundary
    select the largest and smallest seq value for each trip that is within boundary
    WITH query that includes all stops that are within area or stops of routes
    that leaves and then returns to area
    DELETE from stops where not in WITH query
    Cascade for other tables
    :param copy_db_conn:
    :param buffer_lat:
    :param buffer_lon:
    :param buffer_distance:
    :return:
    """

    if (buffer_lat is not None) and (buffer_lon is not None) and (buffer_distance is not None):
        logging.info("Making spatial extract")
        copy_db_conn.create_function("find_distance", 4, wgs84_distance)
        copy_db_conn.execute('DELETE FROM stops '
                             'WHERE stop_I NOT IN '
                             '(SELECT stops.stop_I FROM stop_times, stops, '
                             '(SELECT trip_I, min(seq) AS min_seq, max(seq) AS max_seq FROM stop_times, stops '
                             'WHERE stop_times.stop_I = stops.stop_I '
                             'AND CAST(find_distance(lat, lon, ?, ?) AS INT) < ? '
                             'GROUP BY trip_I) q1 '
                             'WHERE stop_times.stop_I = stops.stop_I '
                             'AND stop_times.trip_I = q1.trip_I '
                             'AND seq >= min_seq '
                             'AND seq <= max_seq '
                             ')', (buffer_lat, buffer_lon, buffer_distance))

        copy_db_conn.execute('DELETE FROM stop_times WHERE '
                             'stop_I NOT IN (SELECT stop_I FROM stops)')
        # delete trips with only one stop
        copy_db_conn.execute('DELETE FROM stop_times WHERE '
                             'trip_I IN (select trip_I from '
                             '(select trip_I, count(*) as N_stops from stop_times '
                             'group by trip_I) q1 '
                             'where N_stops = 1)')

        copy_db_conn.execute('DELETE FROM trips WHERE '
                             'trip_I NOT IN (SELECT trip_I FROM stop_times)')
        copy_db_conn.execute('DELETE FROM routes WHERE '
                             'route_I NOT IN (SELECT route_I FROM trips)')
        copy_db_conn.execute('DELETE FROM agencies WHERE '
                             'agency_I NOT IN (SELECT agency_I FROM routes)')
        copy_db_conn.execute('DELETE FROM shapes WHERE '
                             'shape_id NOT IN (SELECT shape_id FROM trips)')
        copy_db_conn.execute('DELETE FROM stops_rtree WHERE '
                             'stop_I NOT IN (SELECT stop_I FROM stops)')
        copy_db_conn.execute('DELETE FROM stop_distances WHERE '
                             'from_stop_I NOT IN (SELECT stop_I FROM stops)'
                             'OR to_stop_I NOT IN (SELECT stop_I FROM stops)')
        copy_db_conn.commit()
    return


def _update_metadata(copy_db_conn, G_orig, update_metadata, orig_db_path):
    # Update metadata
    if update_metadata:
        logging.info("Updating metadata")
        G_copy = gtfs.GTFS(copy_db_conn)
        G_copy.meta['copied_from'] = orig_db_path
        G_copy.meta['copy_time_ut'] = time.time()
        G_copy.meta['copy_time'] = time.ctime()

        # Copy some keys directly.
        for key in ['original_gtfs',
                    'download_date',
                    'location_name',
                    'timezone', ]:
            G_copy.meta[key] = G_orig.meta[key]
        # Update *all* original metadata under orig_ namespace.
        G_copy.meta.update(('orig_' + k, v) for k, v in G_orig.meta.items())

        stats.update_stats(G_copy)

        # print "Vacuuming..."
        copy_db_conn.execute('VACUUM;')
        # print "Analyzing..."
        copy_db_conn.execute('ANALYZE;')
        copy_db_conn.commit()
    return