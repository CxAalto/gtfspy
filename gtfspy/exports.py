import json
import os
import shutil
import uuid

import networkx
import pandas

from gtfspy import route_types
from gtfspy.gtfs import GTFS
from gtfspy import util
from gtfspy.networks import stop_to_stop_networks_by_type, temporal_network, \
    combined_stop_to_stop_transit_network
from gtfspy.route_types import ROUTE_TYPE_TO_ZORDER


def write_walk_transfer_edges(gtfs, output_file_name):
    """
    Parameters
    ----------
    gtfs: gtfspy.GTFS
    output_file_name: str
    """
    transfers = gtfs.get_table("stop_distances")
    transfers.drop([u"min_transfer_time", u"timed_transfer"], 1, inplace=True)
    with util.create_file(output_file_name, tmpdir=True, keepext=True) as tmpfile:
        transfers.to_csv(tmpfile, encoding='utf-8', index=False)


def write_nodes(gtfs, output, fields=None):
    """
    Parameters
    ----------
    gtfs: gtfspy.GTFS
    output: str
        Path to the output file
    fields: list, optional
        which pieces of information to provide
    """
    nodes = gtfs.get_table("stops")
    if fields is not None:
        nodes = nodes[fields]
    with util.create_file(output, tmpdir=True, keepext=True) as tmpfile:
        nodes.to_csv(tmpfile, encoding='utf-8', index=False, sep=";")


def create_stops_geojson_dict(gtfs, fields=None):
    nodes = gtfs.get_table("stops")
    if fields is None:
        fields = {'name': 'stop_name', 'stop_I': 'stop_I', 'lat': 'lat', 'lon': 'lon'}
    assert (fields['lat'] == 'lat' and fields['lon'] == 'lon')
    nodes = nodes[list(fields.keys())]
    nodes.replace(list(fields.keys()), [fields[key] for key in fields.keys()], inplace=True)
    assert ('lat' in nodes.columns)
    assert ('lon' in nodes.columns)

    features = []
    for i, node_tuple in enumerate(nodes.itertuples()):
        feature = {"type": "Feature",
                   "id": str(i),
                   "geometry": {
                       "type": "Point",
                       "coordinates": [
                           node_tuple.lon,
                           node_tuple.lat
                       ]
                   },
                   "properties": {
                       "stop_I": str(node_tuple.stop_I),
                       "name": node_tuple.name
                   }
                   }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    return geojson

def write_stops_geojson(gtfs, out_file, fields=None):
    """
    Parameters
    ----------
    gtfs: gtfspy.GTFS
    out_file: file-like or path to file
    fields: dict
        simultaneously map each original_name to the new_name
    Returns
    -------
    """
    geojson = create_stops_geojson_dict(gtfs, fields)
    if hasattr(out_file, "write"):
        out_file.write(json.dumps(geojson))
    else:
        with util.create_file(out_file, tmpdir=True, keepext=True) as tmpfile_path:
            tmpfile = open(tmpfile_path, 'w')
            tmpfile.write(json.dumps(geojson))


def write_combined_transit_stop_to_stop_network(gtfs, output_path, fmt=None):
    """
    Parameters
    ----------
    gtfs : gtfspy.GTFS
    output_path : str
    fmt: None, optional
        defaulting to "edg" and writing results as ".edg" files
         If "csv" csv files are produced instead    """
    if fmt is None:
        fmt = "edg"
    multi_di_graph = combined_stop_to_stop_transit_network(gtfs)
    _write_stop_to_stop_network_edges(multi_di_graph, output_path, fmt=fmt)


def write_static_networks(gtfs, output_dir, fmt=None):
    """
    Parameters
    ----------
    gtfs: gtfspy.GTFS
    output_dir: (str, unicode)
        a path where to write
    fmt: None, optional
        defaulting to "edg" and writing results as ".edg" files
         If "csv" csv files are produced instead
    """
    if fmt is None:
        fmt = "edg"
    single_layer_networks = stop_to_stop_networks_by_type(gtfs)
    util.makedirs(output_dir)
    for route_type, net in single_layer_networks.items():
        tag = route_types.ROUTE_TYPE_TO_LOWERCASE_TAG[route_type]
        file_name = os.path.join(output_dir, "network_" + tag + "." + fmt)
        if len(net.edges()) > 0:
            _write_stop_to_stop_network_edges(net, file_name, fmt=fmt)


def write_temporal_networks_by_route_type(gtfs, extract_output_dir):
    """
    Write temporal networks by route type to disk.

    Parameters
    ----------
    gtfs: gtfspy.GTFS
    extract_output_dir: str
    """
    util.makedirs(extract_output_dir)
    for route_type in route_types.TRANSIT_ROUTE_TYPES:
        pandas_data_frame = temporal_network(gtfs, start_time_ut=None, end_time_ut=None, route_type=route_type)
        tag = route_types.ROUTE_TYPE_TO_LOWERCASE_TAG[route_type]
        out_file_name = os.path.join(extract_output_dir, tag + ".tnet")
        pandas_data_frame.to_csv(out_file_name, encoding='utf-8', index=False)


def write_temporal_network(gtfs, output_filename, start_time_ut=None, end_time_ut=None):
    """
    Parameters
    ----------
    gtfs : gtfspy.GTFS
    output_filename : str
        path to the directory where to store the extracts
    start_time_ut: int | None
        start time of the extract in unixtime (seconds after epoch)
    end_time_ut: int | None
        end time of the extract in unixtime (seconds after epoch)
    """
    util.makedirs(os.path.dirname(os.path.abspath(output_filename)))
    pandas_data_frame = temporal_network(gtfs, start_time_ut=start_time_ut, end_time_ut=end_time_ut)
    pandas_data_frame.to_csv(output_filename, encoding='utf-8', index=False)


def _write_stop_to_stop_network_edges(net, file_name, data=True, fmt=None):
    """
    Write out a network

    Parameters
    ----------
    net: networkx.DiGraph
    base_name: str
        path to the filename (without extension)
    data: bool, optional
        whether or not to write out any edge data present
    fmt: str, optional
        If "csv" write out the network in csv format.
    """
    if fmt is None:
        fmt = "edg"

    if fmt == "edg":
        if data:
            networkx.write_edgelist(net, file_name, data=True)
        else:
            networkx.write_edgelist(net, file_name)
    elif fmt == "csv":
        with open(file_name, 'w') as f:
            # writing out the header
            edge_iter = net.edges(data=True)
            _, _, edg_data = next(edge_iter)
            edg_data_keys = list(sorted(edg_data.keys()))
            header = ";".join(["from_stop_I", "to_stop_I"] + edg_data_keys)
            f.write(header)
            for from_node_I, to_node_I, data in net.edges(data=True):
                f.write("\n")
                values = [str(from_node_I), str(to_node_I)]
                data_values = []
                for key in edg_data_keys:
                    if key == "route_I_counts":
                        route_I_counts_string = str(data[key]).replace(" ", "")[1:-1]
                        data_values.append(route_I_counts_string)
                    else:
                        data_values.append(str(data[key]))
                all_values = values + data_values
                f.write(";".join(all_values))


def create_sections_geojson_dict(G, start_time_ut=None, end_time_ut=None):
    multi_di_graph = combined_stop_to_stop_transit_network(G, start_time_ut=start_time_ut, end_time_ut=end_time_ut)
    stops = G.get_table("stops")
    stop_I_to_coords = {row.stop_I: [row.lon, row.lat] for row in stops.itertuples()}
    gjson = {"type": "FeatureCollection"}
    features = []
    gjson["features"] = features
    data = list(multi_di_graph.edges(data=True))
    data.sort(key=lambda el: ROUTE_TYPE_TO_ZORDER[el[2]['route_type']])
    for from_stop_I, to_stop_I, data in data:
        feature = {"type": "Feature"}
        geometry = {
            "type": "LineString",
            'coordinates': [stop_I_to_coords[from_stop_I], stop_I_to_coords[to_stop_I]]
        }
        feature['geometry'] = geometry
        route_I_counts = data['route_I_counts']
        route_I_counts = {str(key): int(value) for key, value in route_I_counts.items()}
        data['route_I_counts'] = route_I_counts
        properties = data
        properties['from_stop_I'] = int(from_stop_I)
        properties['to_stop_I'] = int(to_stop_I)
        feature['properties'] = data
        features.append(feature)
    return gjson

def write_sections_geojson(G, output_file, start_time_ut=None, end_time_ut=None):
    gjson = create_sections_geojson_dict(G, start_time_ut=start_time_ut, end_time_ut=end_time_ut)
    if hasattr(output_file, "write"):
        output_file.write(json.dumps(gjson))
    else:
        with open(output_file, 'w') as f:
            f.write(json.dumps(gjson))

def create_routes_geojson_dict(G):
    assert(isinstance(G, GTFS))
    gjson = {"type": "FeatureCollection"}
    features = []
    for routeShape in G.get_all_route_shapes(use_shapes=False):
        feature = {"type": "Feature"}
        geometry = {
            "type": "LineString",
            "coordinates": list(zip(routeShape['lons'], routeShape['lats']))
        }
        feature['geometry'] = geometry
        properties = {"route_type": int(routeShape['type']),
                      "route_I": int(routeShape['route_I']),
                      "route_name": str(routeShape['name'])}
        feature['properties'] = properties
        features.append(feature)
    gjson['features'] = features
    return gjson

def write_routes_geojson(G, output_file):
    gjson = create_routes_geojson_dict(G)
    if hasattr(output_file, "write"):
        output_file.write(json.dumps(gjson))
    else:
        with open(output_file, 'w') as f:
            f.write(json.dumps(gjson))
    return None



def write_gtfs(gtfs, output):
    """
    Write out the database according to the GTFS format.

    Parameters
    ----------
    gtfs: gtfspy.GTFS
    output: str
        Path where to put the GTFS files
        if output ends with ".zip" a ZIP-file is created instead.

    Returns
    -------
    None
    """
    output = os.path.abspath(output)
    uuid_str = "tmp_" + str(uuid.uuid1())
    if output[-4:] == '.zip':
        zip = True
        out_basepath = os.path.dirname(os.path.abspath(output))
        if not os.path.exists(out_basepath):
            raise IOError(out_basepath + " does not exist, cannot write gtfs as a zip")
        tmp_dir = os.path.join(out_basepath, str(uuid_str))
        # zip_file_na,e = ../out_basedir + ".zip
    else:
        zip = False
        out_basepath = output
        tmp_dir = os.path.join(out_basepath + "_" + str(uuid_str))

    os.makedirs(tmp_dir, exist_ok=True)

    gtfs_table_to_writer = {
        "agency": _write_gtfs_agencies,
        "calendar": _write_gtfs_calendar,
        "calendar_dates": _write_gtfs_calendar_dates,
        # fare attributes and fare_rules omitted (seldomly used)
        "feed_info": _write_gtfs_feed_info,
        # "frequencies": not written, as they are incorporated into trips and routes,
        # Frequencies table is expanded into other tables on initial import. -> Thus frequencies.txt is not created
        "routes": _write_gtfs_routes,
        "shapes": _write_gtfs_shapes,
        "stops": _write_gtfs_stops,
        "stop_times": _write_gtfs_stop_times,
        "transfers": _write_gtfs_transfers,
        "trips": _write_gtfs_trips,
    }

    for table, writer in gtfs_table_to_writer.items():
        fname_to_write = os.path.join(tmp_dir, table + '.txt')
        print(fname_to_write)
        writer(gtfs, open(os.path.join(tmp_dir, table + '.txt'), 'w'))

    if zip:
        shutil.make_archive(output[:-4], 'zip', tmp_dir)
        shutil.rmtree(tmp_dir)
    else:
        print("moving " + str(tmp_dir) + " to " + out_basepath)
        os.rename(tmp_dir, out_basepath)



def _remove_I_columns(df):
    """
    Remove columns ending with I from a pandas.DataFrame

    Parameters
    ----------
    df: dataFrame

    Returns
    -------
    None
    """
    all_columns = list(filter(lambda el: el[-2:] == "_I", df.columns))
    for column in all_columns:
        del df[column]


def __replace_I_with_id(gtfs, current_table, from_table_name, old_column_current, old_column_from, new_column_in_from,
                        new_column_name=None):
    if new_column_name is None:
        new_column_name = new_column_in_from
    from_table = gtfs.get_table(from_table_name)
    merged = pandas.merge(current_table, from_table, how="left",
                          left_on=old_column_current, right_on=old_column_from)
    series = pandas.Series(merged[new_column_in_from], index=current_table.index)
    current_table[new_column_name] = series


def _write_gtfs_agencies(gtfs, output_file):
    # remove agency_I
    agencies_table = gtfs.get_table("agencies")
    assert (isinstance(agencies_table, pandas.DataFrame))
    columns_to_change = {'name': 'agency_name',
                         'url': 'agency_url',
                         'timezone': 'agency_timezone',
                         'lang': 'agency_lang',
                         'phone': 'agency_phone'}
    agencies_table = agencies_table.rename(columns=columns_to_change)
    _remove_I_columns(agencies_table)
    agencies_table.to_csv(output_file, index=False)


def _write_gtfs_stops(gtfs, output_file):
    stops_table = gtfs.get_table("stops")
    assert (isinstance(stops_table, pandas.DataFrame))
    columns_to_change = {'name': 'stop_name',
                         'url': 'stop_url',
                         'lat': 'stop_lat',
                         'lon': 'stop_lon',
                         'code': 'stop_code',
                         'desc': 'stop_desc'
                         }
    stops_table = stops_table.rename(columns=columns_to_change)

    # Remove stop_I
    stop_I_to_id = {row.stop_I: row.stop_id for row in stops_table.itertuples()}
    parent_stations = []
    for stop_row in stops_table.itertuples():
        try:
            parent_station = stop_I_to_id[stop_row.parent_I]
        except KeyError:
            parent_station = ""
        parent_stations.append(parent_station)
    stops_table['parent_station'] = pandas.Series(parent_stations, index=stops_table.index)
    _remove_I_columns(stops_table)
    stops_table.to_csv(output_file, index=False)


def _write_gtfs_routes(gtfs, output_file):
    routes_table = gtfs.get_table("routes")
    columns_to_change = {'name': 'route_short_name',
                         'long_name': 'route_long_name',
                         'url': 'route_url',
                         'type': 'route_type',
                         'desc': 'route_desc',
                         'color': 'route_color',
                         'text_color': 'route_text_color'
                         }
    routes_table = routes_table.rename(columns=columns_to_change)

    # replace agency_I
    agencies_table = gtfs.get_table("agencies")
    agency_ids = pandas.merge(routes_table, agencies_table, how="left", on='agency_I')['agency_id']
    routes_table['agency_id'] = pandas.Series(agency_ids, index=routes_table.index)
    _remove_I_columns(routes_table)
    routes_table.to_csv(output_file, index=False)


def _write_gtfs_trips(gtfs, output_file):
    trips_table = gtfs.get_table("trips")
    columns_to_change = {
        'headsign': 'trip_headsign',
    }
    trips_table = trips_table.rename(columns=columns_to_change)

    __replace_I_with_id(gtfs, trips_table, 'routes', 'route_I', 'route_I', 'route_id')
    __replace_I_with_id(gtfs, trips_table, 'calendar', 'service_I', 'service_I', 'service_id')

    _remove_I_columns(trips_table)
    del [trips_table['start_time_ds']]
    del [trips_table['end_time_ds']]

    trips_table.to_csv(output_file, index=False)


def _write_gtfs_stop_times(gtfs, output_file):
    stop_times_table = gtfs.get_table('stop_times')

    columns_to_change = {
        'seq': 'stop_sequence',
        'arr_time': 'arrival_time',
        'dep_time': 'departure_time'
    }
    stop_times_table = stop_times_table.rename(columns=columns_to_change)

    # replace trip_I with trip_id
    __replace_I_with_id(gtfs, stop_times_table, 'trips', 'trip_I', 'trip_I', 'trip_id')
    __replace_I_with_id(gtfs, stop_times_table, 'stops', 'stop_I', 'stop_I', 'stop_id')

    # delete unneeded columns:
    del [stop_times_table['arr_time_hour']]
    del [stop_times_table['arr_time_ds']]
    del [stop_times_table['dep_time_ds']]
    del [stop_times_table['shape_break']]
    _remove_I_columns(stop_times_table)

    stop_times_table.to_csv(output_file, index=False)


def _write_gtfs_calendar(gtfs, output_file):
    calendar_table = gtfs.get_table('calendar')
    columns_to_change = {
        'm': 'monday',
        't': 'tuesday',
        'w': 'wednesday',
        'th': 'thursday',
        'f': 'friday',
        's': 'saturday',
        'su': 'sunday'
    }
    calendar_table = calendar_table.rename(columns=columns_to_change)
    calendar_table['start_date'] = [date.replace("-", "") for date in calendar_table['start_date']]
    calendar_table['end_date'] = [date.replace("-", "") for date in calendar_table['end_date']]
    _remove_I_columns(calendar_table)
    calendar_table.to_csv(output_file, index=False)


def _write_gtfs_calendar_dates(gtfs, output_file):
    calendar_dates_table = gtfs.get_table('calendar_dates')
    __replace_I_with_id(gtfs, calendar_dates_table, 'calendar', 'service_I', 'service_I', 'service_id')
    _remove_I_columns(calendar_dates_table)
    calendar_dates_table.to_csv(output_file, index=False)


def _write_gtfs_shapes(gtfs, ouput_file):
    shapes_table = gtfs.get_table('shapes')
    columns_to_change = {
        'lat': 'shape_pt_lat',
        'lon': 'shape_pt_lon',
        'seq': 'shape_pt_sequence',
        'd': 'shape_dist_traveled'
    }
    shapes_table = shapes_table.rename(columns=columns_to_change)
    shapes_table.to_csv(ouput_file, index=False)


def _write_gtfs_feed_info(gtfs, output_file):
    gtfs.get_table('feed_info').to_csv(output_file, index=False)


def _write_gtfs_frequencies(gtfs, output_file):
    raise NotImplementedError("Frequencies should not be outputted from GTFS as they are included in other tables.")


def _write_gtfs_transfers(gtfs, output_file):
    transfers_table = gtfs.get_table('transfers')
    __replace_I_with_id(gtfs, transfers_table, 'stops', 'from_stop_I', 'stop_I', 'stop_id', 'from_stop_id')
    __replace_I_with_id(gtfs, transfers_table, 'stops', 'to_stop_I', 'stop_I', 'stop_id', 'to_stop_id')
    _remove_I_columns(transfers_table)
    transfers_table.to_csv(output_file, index=False)


def _write_gtfs_stop_distances(gtfs, output_file):
    stop_distances = gtfs.get_table('stop_distances')
    __replace_I_with_id(gtfs, stop_distances, 'stops', 'from_stop_I', 'stop_I', 'stop_id', 'from_stop_id')
    __replace_I_with_id(gtfs, stop_distances, 'stops', 'to_stop_I', 'stop_I', 'stop_id', 'to_stop_id')
    _remove_I_columns(stop_distances)
    del stop_distances['min_transfer_time']
    del stop_distances['timed_transfer']
    stop_distances.to_csv(output_file, index=False)


# for row in stop_times_table.itertuples():
#     dep_time = gtfs.unixtime_seconds_to_gtfs_datetime(row.dep_time_ds).strftime('%H:%M%S')
#     arr_time = gtfs.unixtime_seconds_to_gtfs_datetime(row.arr_time_ds).strftime('%H:%M%S')
#     departure_times.append(dep_time)
#     arrival_times.append(arr_time)
# stop_times_table['arrival_time'] = pandas.Series(arrival_times, stop_times_table.index)
# stop_times_table['departure_time'] = pandas.Series(departure_times, stop_times_table.index)




def main():
    import argparse

    parser = argparse.ArgumentParser(description="Create network extracts from already imported GTFS files.")
    subparsers = parser.add_subparsers(dest='cmd')

    # parsing import
    parser_routingnets = subparsers.add_parser('extract_temporal', help="Direct import GTFS->sqlite")
    parser_routingnets.add_argument('gtfs', help='Input GTFS .sqlite (must end in .sqlite)')
    parser_routingnets.add_argument('basename', help='Basename for the output files')  # Parsing copy

    args = parser.parse_args()

    # if the first argument is import, import a GTFS directory to a .sqlite database.
    # Both directory and
    if args.cmd == 'extract_temporal':
        gtfs_fname = args.gtfs
        output_basename = args.basename

        from gtfspy.gtfs import GTFS
        gtfs = GTFS(gtfs_fname)

        nodes_filename = output_basename + ".nodes.csv"
        with util.create_file(nodes_filename, tmpdir=True, keepext=True) as tmpfile:
            write_nodes(gtfs, tmpfile)

        transfers_filename = output_basename + ".transfers.csv"
        with util.create_file(transfers_filename, tmpdir=True, keepext=True) as tmpfile:
            write_walk_transfer_edges(gtfs, tmpfile)

        temporal_network_filename = output_basename + ".temporal_network.csv"
        with util.create_file(temporal_network_filename, tmpdir=True, keepext=True) as tmpfile:
            write_temporal_network(gtfs, tmpfile)

    else:
        print("Unrecognized command: %s" % args.cmd)
        exit(1)


if __name__ == "__main__":
    main()


