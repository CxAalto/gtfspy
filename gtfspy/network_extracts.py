import os

import networkx

from gtfspy import route_types
from gtfspy import util
from gtfspy.networks import walk_transfer_stop_to_stop_network, stop_to_stop_networks_by_type, temporal_network, \
    combined_stop_to_stop_transit_network


def write_walk_transfer_edges(gtfs, output):
    """
    Note: either @param:gtfs, or @param:net must be specified.

    Parameters
    ----------
    gtfs: gtfspy.GTFS
        Not used if @param:net is given
    output: str

    Return
    ------
    net: networkx.Graph
    """
    net = walk_transfer_stop_to_stop_network(gtfs)
    u, v, data = next(net.edges_iter(data=True))
    keys = list(data.keys()).sort()
    with util.create_file(output, tmpdir=True, keepext=True) as tmpfile:
        tmpfile.write("#from to " + " ".join(keys))
        networkx.write_edgelist(net, tmpfile, data=keys)


def write_nodes(gtfs, output):
    """
    Note: either @param:gtfs, or @param:net must be specified.

    Parameters
    ----------
    gtfs: gtfspy.GTFS
        Not used if @param:net is given
    output: str
        Path to the output file
    """
    nodes = gtfs.get_table("stops")
    with util.create_file(output, tmpdir=True, keepext=True) as tmpfile:
        nodes.to_csv(tmpfile)


def write_combined_transit_stop_to_stop_network(gtfs, extract_output_dir):
    """
    Parameters
    ----------
    gtfs : gtfspy.GTFS
    extract_output_dir : str
    """
    multi_di_graph = combined_stop_to_stop_transit_network(gtfs)
    util.makedirs(extract_output_dir)
    _write_stop_to_stop_network(multi_di_graph, os.path.join(extract_output_dir, "combined"))


def write_stop_to_stop_networks(gtfs, output_dir):
    """
    Parameters
    ----------
    gtfs: gtfspy.GTFS
    output_dir: (str, unicode)
        a path where to write
    """
    single_layer_networks = stop_to_stop_networks_by_type(gtfs)
    util.makedirs(output_dir)
    for route_type, net in single_layer_networks.iteritems():
        tag = route_types.ROUTE_TYPE_TO_LOWERCASE_TAG[route_type]
        base_name = os.path.join(output_dir, tag)
        _write_stop_to_stop_network(net, base_name)


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
        pandas_data_frame = temporal_network(gtfs, start_time_ut=None, end_time_ut=None)
        tag = route_types.ROUTE_TYPE_TO_LOWERCASE_TAG[route_type]
        out_file_name = os.path.join(extract_output_dir, tag + ".tnet")
        pandas_data_frame.to_csv(out_file_name)


def write_temporal_network(gtfs, output_filename, start_time_ut=None, end_time_ut=None):
    """
    Parameters
    ----------
    gtfs : gtfspy.GTFS
    output_filename : str
        path to the directory where to store teh extracts
    start_time_ut: int | None
        start time of the extract in unixtime (seconds after epoch)
    end_time_ut: int | None
        end time of the extract in unixtime (seconds after epoch)
    """
    util.makedirs(os.path.dirname(os.path.abspath(output_filename)))
    pandas_data_frame = temporal_network(gtfs, start_time_ut=start_time_ut, end_time_ut=end_time_ut)
    pandas_data_frame.to_csv(output_filename)


def _write_stop_to_stop_network(net, base_name, data=True):
    """
    Write out a network

    Parameters
    ----------
    net: networkx.DiGraph
    base_name: str
        path to the filename (without extension)
    """
    if data:
        networkx.write_edgelist(net, base_name + "_with_data.edg", data=True)
    else:
        networkx.write_edgelist(net, base_name + ".edg")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Create network extracts from already imported GTFS files.")
    subparsers = parser.add_subparsers(dest='cmd')

    # parsing import
    parser_routingnets = subparsers.add_parser('extract_routingnets', help="Direct import GTFS->sqlite")
    parser_routingnets.add_argument('gtfs', help='Input GTFS .sqlite (must end in .sqlite)')
    parser_routingnets.add_argument('destdir', help='Output directory for any extracts produced')  # Parsing copy

    args = parser.parse_args()

    # if the first argument is import, import a GTFS directory to a .sqlite database.
    # Both directory and
    if args.cmd == 'extract_routingnets':
        gtfs_fname = args.gtfs
        destdir = args.destdir

        from gtfspy.gtfs import GTFS
        gtfs = GTFS(gtfs_fname)

        nodes_filename = os.path.join(destdir, "nodes.csv")
        with util.create_file(nodes_filename, tmpdir=True, keepext=True) as tmpfile:
            write_nodes(gtfs, tmpfile)

        transfers_filename = os.path.join(destdir, "transfers.edg")
        with util.create_file(transfers_filename, tmpdir=True, keepext=True) as tmpfile:
            write_walk_transfer_edges(gtfs, tmpfile)

        temporal_network_filename = os.path.join(destdir, "temporal_network.csv")
        with util.create_file(temporal_network_filename , tmpdir=True, keepext=True) as tmpfile:
            write_temporal_network(gtfs, tmpfile)

    else:
        print("Unrecognized command: %s" % args.cmd)
        exit(1)


if __name__ == "__main__":
    main()
