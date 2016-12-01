"""Various utility functions"""
from __future__ import print_function

import contextlib
import math
from math import cos
import datetime
import os
import shutil
import tempfile
import time

import networkx as nx

# Below is a race condition, so do it only on import.  Is there a
# portable way to do this?
current_umask = os.umask(0)
os.umask(current_umask)

# Various unrelated utility functions.

TORADIANS = 3.141592653589793 / 180.
EARTH_RADIUS = 6378137.


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
    return meters/(EARTH_RADIUS * TORADIANS)


def wgs84_width(meters, lat):
    R2 = EARTH_RADIUS * cos(lat*TORADIANS)
    return meters/(R2 * TORADIANS)

# cython implementation of this.  It is called _often_.
try:
    from cutil import wgs84_distance
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
            root = root + ext
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
            prefix='tmp-'+root+'-', suffix=ext, dir=dir_, delete=False)
        fname_tmp = tmpfile.name
    try:
        yield fname_tmp
    except Exception as e:
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
    except OSError as e:
        # New temporary file in same directory
        tmpfile2 = tempfile.NamedTemporaryFile(
            prefix='tmp-'+root+'-', suffix=ext, dir=this_dir, delete=False)
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


def ut_to_utc_datetime_str(time_ut):
    dt = datetime.datetime.utcfromtimestamp(time_ut)
    return dt.strftime("%b %d %Y %H:%M:%S")


def makedirs(path):
    """
    Create directories if they do not exist, otherwise do nothing.

    Return path for convenience
    """
    if not os.path.isdir(path):
        os.makedirs(path)
    return path


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
    nx.draw(net, pos=node_coords, ax=ax, node_size=50)
    return fig


def day_seconds_to_str_time(ds):
    assert ds >= 0
    hours = ds // 3600
    minutes = (ds - hours * 3600) // 60
    seconds = ds % 60
    return "%02d:%02d:%02d" % (hours, minutes, seconds)


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
        print('timeit: %r %2.2f sec (%r, %r) ' % (method.__name__, time_end-time_start, args, kw))
        return result

    return timed


def corrupted_zip(zip_path):
    import zipfile
    try:
        zip_to_test = zipfile.ZipFile(zip_path)
        #warning = zip_to_test.testzip()
        #if warning is not None:
        #    return str(warning)
        #else:
        return "ok"
    except:
        return "error"
