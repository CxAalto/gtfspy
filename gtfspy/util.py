import contextlib
import ctypes
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
import shapefile as shp

"""
Various unrelated utility functions.
"""

# Below is a race condition, so do it only on import.
# Is there a portable way to do this?
current_umask = os.umask(0)
os.umask(current_umask)

TORADIANS = 3.141592653589793 / 180.0
EARTH_RADIUS = 6378137.0


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

    if sys.platform == 'win32': # tzset() does not work on Windows
        system_time = SystemTime()
        lpSystemTime = ctypes.pointer(system_time)
        ctypes.windll.kernel32.GetLocalTime(lpSystemTime)
    else:
        time.tzset()  # Cause C-library functions to notice the update.

    return prev_timezone


class SystemTime(ctypes.Structure):
    _fields_ = [
        ('wYear', ctypes.c_int16),
        ('wMonth', ctypes.c_int16),
        ('wDayOfWeek', ctypes.c_int16),
        ('wDay', ctypes.c_int16),
        ('wHour', ctypes.c_int16),
        ('wMinute', ctypes.c_int16),
        ('wSecond', ctypes.c_int16),
        ('wMilliseconds', ctypes.c_int16)]


def wgs84_distance(lat1, lon1, lat2, lon2):
    """Distance (in meters) between two points in WGS84 coord system."""
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = EARTH_RADIUS * c
    return d


def wgs84_height(meters):
    return meters / (EARTH_RADIUS * TORADIANS)


def wgs84_width(meters, lat):
    R2 = EARTH_RADIUS * cos(lat * TORADIANS)
    return meters / (R2 * TORADIANS)


# cython implementation of this.  It is called _often_.
try:
    from gtfspy.cutil import wgs84_distance
except ImportError:
    pass

possible_tmpdirs = ["/tmp", ""]


@contextlib.contextmanager
def create_file(fname=None, fname_tmp=None, tmpdir=None, save_tmpfile=False, keepext=False):
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
    # Do nothing if requesting sqlite memory DB.
    if fname == ":memory:":
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
            root = root + ext
            ext = ""
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
            prefix="tmp-" + root + "-", suffix=ext, dir=dir_, delete=False
        )
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
            prefix="tmp-" + root + "-", suffix=ext, dir=this_dir, delete=False
        )
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
        stmt = stmt.replace("%", "%%").replace("?", "%r")
        print(stmt % (args[1]))
    return cur.execute(*args)


def to_date_string(date):
    if isinstance(date, numpy.int64) or isinstance(date, int):
        date = str(date)
        date = "%s-%s-%s" % (date[:4], date[4:6], date[6:8])
        return date


def ut_to_utc_datetime_str(time_ut):
    dt = datetime.datetime.utcfromtimestamp(time_ut)
    return dt.strftime("%b %d %Y %H:%M:%S")


def str_time_to_day_seconds(time):
    """
    Converts time strings to integer seconds
    :param time: %H:%M:%S string
    :return: integer seconds
    """
    t = str(time).split(":")
    seconds = int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])
    return seconds


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
        print("timeit: %r %2.2f sec " % (method.__name__, time_end - time_start))
        return result

    return timed


def corrupted_zip(zip_path):
    import zipfile

    try:
        zipfile.ZipFile(zip_path)
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
    if ".txt" not in table:
        table += ".txt"

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


def write_shapefile(data, shapefile_path):
    from numpy import int64

    """
    :param data: list of dicts where dictionary contains the keys lons and lats
    :param shapefile_path: path where shapefile is saved
    :return:
    """

    w = shp.Writer(shp.POLYLINE)  # shapeType=3)

    fields = []

    # This makes sure every geom has all the attributes
    w.autoBalance = 1
    # Create all attribute fields except for lats and lons. In addition the field names are saved for the
    # datastoring phase. Encode_strings stores .encode methods as strings for all fields that are strings
    if not fields:
        for key, value in data[0].items():
            if key != "lats" and key != "lons":
                fields.append(key)

                if type(value) == float:
                    w.field(key.encode("ascii"), fieldType="N", size=11, decimal=3)
                    print("float", type(value))
                elif type(value) == int or type(value) == int64:
                    print("int", type(value))

                    # encode_strings.append(".encode('ascii')")
                    w.field(key.encode("ascii"), fieldType="N", size=6, decimal=0)
                else:
                    print("other type", type(value))

                    w.field(key.encode("ascii"))

    for dict_item in data:
        line = []
        lineparts = []
        records_string = ""
        for lat, lon in zip(dict_item["lats"], dict_item["lons"]):
            line.append([float(lon), float(lat)])
        lineparts.append(line)
        w.line(parts=lineparts)

        # The shapefile records command is built up as strings to allow a differing number of columns
        for field in fields:
            if records_string:
                records_string += ", dict_item['" + field + "']"
            else:
                records_string += "dict_item['" + field + "']"
        method_string = "w.record(" + records_string + ")"

        # w.record(dict_item['name'], dict_item['agency'], dict_item['agency_name'], dict_item['type'], dict_item['lons'])
        print(method_string)
        eval(method_string)
    w.save(shapefile_path)


# Opening files with Universal newlines is done differently in py3
def zip_open(z, filename):
    if sys.version_info[0] == 2:
        return z.open(filename, "rU")
    else:
        return io.TextIOWrapper(z.open(filename, 'r'), "utf-8")


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
        node_coords[node] = (data["lon"], data["lat"])
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
    Returns a dataframe with all of df_other that are not in df_self, when considering the columns specified in col_names
    :param df_self: pandas Dataframe
    :param df_other: pandas Dataframe
    :param col_names: list of column names
    :return:
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
