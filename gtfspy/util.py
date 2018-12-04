import contextlib
import datetime
import io
import math
import os
import shutil
import sys
import tempfile
import time
import zipfile
from math import cos

import networkx
import numpy
import pandas as pd

from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, MultiPolygon
from shapely.wkt import loads
from geopandas import GeoDataFrame

"""
Various unrelated utility functions.
"""

# Below is a race condition, so do it only on import.
# Is there a portable way to do this?
current_umask = os.umask(0)
os.umask(current_umask)

TORADIANS = 3.141592653589793 / 180.
EARTH_RADIUS = 6378137.
crs_wgs = {'init': 'epsg:4326'}


def set_process_timezone(TZ):
    """
    Parameters
    ----------
    TZ: string
    """
    try:
        prev_timezone = os.environ['TZ']
    except KeyError:
        prev_timezone = None
    os.environ['TZ'] = TZ
    time.tzset()  # Cause C-library functions to notice the update.
    return prev_timezone


def wgs84_distance(lat1, lon1, lat2, lon2):
    """Distance (in meters) between two points in WGS84 coord system."""
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat / 2) * math.sin(dLat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon / 2) * math.sin(dLon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = EARTH_RADIUS * c
    return d


def wgs84_height(meters):
    return meters / (EARTH_RADIUS * TORADIANS)


def wgs84_width(meters, lat):
    R2 = EARTH_RADIUS * cos(lat * TORADIANS)
    return meters / (R2 * TORADIANS)


def get_utm_srid_from_wgs(lon, lat):
    """this can be used for quicker distance calculations with projections using meters as native distance unit"""
    # convert_wgs_to_utm function, see https://stackoverflow.com/questions/40132542/get-a-cartesian-projection-accurate-around-a-lat-lng-pair
    utm_band = str((math.floor((lon + 180) / 6) % 60) + 1)
    if len(utm_band) == 1:
        utm_band = '0'+utm_band
    if lat >= 0:
        epsg_code = '326' + utm_band
    else:
        epsg_code = '327' + utm_band
    return epsg_code

# cython implementation of this.  It is called _often_.
try:
    from gtfspy.cutil import wgs84_distance
except ImportError:
    pass

possible_tmpdirs = [
    '/tmp',
    ''
]


@contextlib.contextmanager
def create_file(fname=None, fname_tmp=None, tmpdir=None,
                save_tmpfile=False, keepext=False):
    """Context manager for making files with possibility of failure.

    If you are creating a file, it is possible that the code will fail
    and leave a corrupt intermediate file.  This is especially damaging
    if this is used as automatic input to another process.  This context
    manager helps by creating a temporary filename, your code runs and
    creates that temporary file, and then if no exceptions are raised,
    the context manager will move the temporary file to the original
    filename you intended to open.

    Parameters
    ----------
    fname : str
        Target filename, this file will be created if all goes well
    fname_tmp : str
        If given, this is used as the temporary filename.
    tmpdir : str or bool
        If given, put temporary files in this directory.  If `True`,
        then find a good tmpdir that is not on local filesystem.
    save_tmpfile : bool
        If true, the temporary file is not deleteted if an exception
        is raised.
    keepext : bool, default False
            If true, have tmpfile have same extension as final file.

    Returns (as context manager value)
    ----------------------------------
     fname_tmp: str
        Temporary filename to be used.  Same as `fname_tmp`
        if given as an argument.

    Raises
    ------
    Re-raises any except occuring during the context block.
    """
    root = None
    ext = None
    this_dir = None
    # Do nothing if requesting sqlite memory DB.
    if fname == ':memory:':
        yield fname
        return
    if fname_tmp is None:
        # no tmpfile name given - compute some basic info
        basename = os.path.basename(fname)
        root, ext = os.path.splitext(basename)
        dir_ = this_dir = os.path.dirname(fname)
        # Remove filename extension, in case this matters for
        # automatic things itself.
        if not keepext:
            root += ext
            ext = ''
        if tmpdir:
            # we should use a different temporary directory
            if tmpdir is True:
                # Find a directory ourself, searching some common
                # places.
                for dir__ in possible_tmpdirs:
                    if os.access(dir__, os.F_OK):
                        dir_ = dir__
                        break
        # Make the actual tmpfile, with our chosen tmpdir, directory,
        # extension.  Set it to not delete automatically, since on
        # success we will move it to elsewhere.
        tmpfile = tempfile.NamedTemporaryFile(
            prefix='tmp-' + root + '-', suffix=ext, dir=dir_, delete=False)
        fname_tmp = tmpfile.name
    try:
        yield fname_tmp
    except Exception:
        if save_tmpfile:
            print("Temporary file is '%s'" % fname_tmp)
        else:
            os.unlink(fname_tmp)
        raise
    # Move the file back to the original location.
    try:
        os.rename(fname_tmp, fname)
        # We have to manually set permissions.  tempfile does not use
        # umask, for obvious reasons.
        os.chmod(fname, 0o777 & ~current_umask)
    # 'Invalid cross-device link' - you can't rename files across
    # filesystems.  So, we have to fallback to moving it.  But, we
    # want to move it using tmpfiles also, so that the final file
    # appearing is atomic.  We use... tmpfiles.
    except OSError:
        # New temporary file in same directory
        tmpfile2 = tempfile.NamedTemporaryFile(
            prefix='tmp-' + root + '-', suffix=ext, dir=this_dir, delete=False)
        # Copy contents over
        shutil.copy(fname_tmp, tmpfile2.name)
        # Rename new tmpfile, unlink old one on other filesystem.
        os.rename(tmpfile2.name, fname)
        os.chmod(fname, 0o666 & ~current_umask)
        os.unlink(fname_tmp)


def execute(cur, *args):
    """Utility function to print sqlite queries before executing.

    Use instead of cur.execute().  First argument is cursor.

    cur.execute(stmt)
    becomes
    util.execute(cur, stmt)
    """
    stmt = args[0]
    if len(args) > 1:
        stmt = stmt.replace('%', '%%').replace('?', '%r')
        print(stmt % (args[1]))
    return cur.execute(*args)


def to_date_string(date):
    if isinstance(date, numpy.int64) or isinstance(date, int):
        date = str(date)
        date = '%s-%s-%s' % (date[:4], date[4:6], date[6:8])
        return date


def ut_to_utc_datetime_str(time_ut):
    dt = datetime.datetime.utcfromtimestamp(time_ut)
    return dt.strftime("%b %d %Y %H:%M:%S")


def ut_to_utc_datetime(time_ut, tz=None, as_string=False):
    if as_string:
        dt = datetime.datetime.fromtimestamp(time_ut, tz)
        return dt.strftime("%b %d %Y %H:%M:%S")
    else:
        return datetime.datetime.fromtimestamp(time_ut, tz)


def str_time_to_day_seconds(time_string):
    """
    Converts time strings to integer seconds

    Parameters
    ----------
    time_string: str
        in format: %H:%M:%S

    Returns
    -------
    day_seconds: int
        Number of seconds since the day beginning.
    """
    t = str(time_string).split(':')
    day_seconds = int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])
    return day_seconds


def day_seconds_to_str_time(ds):
    assert ds >= 0
    hours = ds // 3600
    minutes = (ds - hours * 3600) // 60
    seconds = ds % 60
    return "%02d:%02d:%02d" % (hours, minutes, seconds)


def makedirs(path):
    """
    Create directories if they do not exist, otherwise do nothing.

    Return path for convenience
    """
    if not os.path.isdir(path):
        os.makedirs(path)
    return path


def timeit(method):
    """
    A Python decorator for printing out the execution time for a function.

    Adapted from:
    www.andreas-jung.com/contents/a-python-decorator-for-measuring-the-execution-time-of-methods
    """

    def timed(*args, **kw):
        time_start = time.time()
        result = method(*args, **kw)
        time_end = time.time()
        print('timeit: %r %2.2f sec ' % (method.__name__, time_end - time_start))
        return result

    return timed


def corrupted_zip(zip_path):
    import zipfile
    try:
        # noinspection PyUnusedLocal
        zip_to_test = zipfile.ZipFile(zip_path)
        # warning = zip_to_test.testzip()
        # if warning is not None:
        #    return str(warning)
        # else:
        return "ok"
    except:
        return "error"


def source_csv_to_pandas(path, table, read_csv_args=None):
    """
    Parameters
    ----------
    path: str
        path to directory or zipfile
    table: str
        name of table
    read_csv_args:
        string arguments passed to the read_csv function

    Returns
    -------
    df: pandas:DataFrame
    """
    if '.txt' not in table:
        table += '.txt'

    if isinstance(path, dict):
        data_obj = path[table]
        f = data_obj.split("\n")
    else:
        if os.path.isdir(path):
            f = open(os.path.join(path, table))

        else:

            z = zipfile.ZipFile(path)
            for path in z.namelist():
                if table in path:
                    table = path
                    break
            try:
                f = zip_open(z, table)
            except KeyError:
                return pd.DataFrame()

    if read_csv_args:
        df = pd.read_csv(**read_csv_args)
    else:
        df = pd.read_csv(f)
    return df


# Opening files with Universal newlines is done differently in py3
def zip_open(z, filename):
    if sys.version_info[0] == 2:
        return z.open(filename, 'rU')
    else:
        return io.TextIOWrapper(z.open(filename, 'r'))


def draw_net_using_node_coords(net):
    """
    Plot a networkx.Graph by using the lat and lon attributes of nodes.
    Parameters
    ----------
    net : networkx.Graph
    Returns
    -------
    fig : matplotlib.figure
        the figure object where the network is plotted
    """
    import matplotlib.pyplot as plt
    fig = plt.figure()
    node_coords = {}
    for node, data in net.nodes(data=True):
        node_coords[node] = (data['lon'], data['lat'])
    ax = fig.add_subplot(111)
    networkx.draw(net, pos=node_coords, ax=ax, node_size=50)
    return fig


def make_sure_path_exists(path):
    import os
    import errno
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def difference_of_pandas_dfs(df_self, df_other, col_names=None):
    """
    Returns a dataframe with all of df_other that are not in df_self
    when considering the columns specified in col_names

    Parameters
    ----------
    df_self: pandas.Dataframe
    df_other: pandas.Dataframe
    col_names: list
        list of column names
    """
    df = pd.concat([df_self, df_other])
    df = df.reset_index(drop=True)
    df_gpby = df.groupby(col_names)
    idx = [x[0] for x in list(df_gpby.groups.values()) if len(x) == 1]
    df_sym_diff = df.reindex(idx)
    df_diff = pd.concat([df_other, df_sym_diff])
    df_diff = df_diff.reset_index(drop=True)
    df_gpby = df_diff.groupby(col_names)
    idx = [x[0] for x in list(df_gpby.groups.values()) if len(x) == 2]
    df_diff = df_diff.reindex(idx)
    return df_diff


def df_to_utm_gdf(df):
    """
    Converts pandas dataframe with lon and lat columns to a geodataframe with a UTM projection
    :param df:
    :return:
    """
    if "wkt" in list(df):
        df["geometry"] = df["wkt"].apply(lambda x: loads(x))

    elif "lat" in list(df) and "lon" in list(df):
        df["geometry"] = df.apply(lambda row: Point((row["lon"], row["lat"])), axis=1)

    elif all(["from_lon" in list(df), "from_lat" in list(df), "to_lon" in list(df), "to_lat" in list(df)]):
        df["geometry"] = df.apply(lambda row: LineString([Point(row.from_lon, row.from_lat),
                                                          Point(row.to_lon, row.to_lat)]), axis=1)
    else:
        raise NameError

    gdf = GeoDataFrame(df, crs=crs_wgs, geometry=df["geometry"])
    if gdf.geom_type[0] == 'Point':
        origin_centroid = MultiPoint(gdf["geometry"].tolist()).centroid
    elif gdf.geom_type[0] == 'LineString':
        origin_centroid = MultiLineString(gdf["geometry"].tolist()).centroid
    elif gdf.geom_type[0] == 'Polygon':
        origin_centroid = MultiPolygon(gdf["geometry"].tolist()).centroid
    else:
        raise NameError

    crs = {'init': 'epsg:{srid}'.format(srid=get_utm_srid_from_wgs(origin_centroid.x, origin_centroid.y))}

    gdf = gdf.to_crs(crs=crs)
    return gdf, crs


def utm_to_wgs(gdf):
    gdf = gdf.to_crs(crs_wgs)
    gdf["lat"] = gdf.apply(lambda row: row.geometry.y, axis=1)
    gdf["lon"] = gdf.apply(lambda row: row.geometry.x, axis=1)
    gdf = gdf.drop('geometry', 1)
    return gdf
