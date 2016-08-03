"""Search for and open databases.

This file provides some functions for opening database files.  This is a
bit more complex than it seems, since it has to search in different
places for them, both on personal computers and BECS computers.  On BECS
computers, there are also the DBs in scratch, and the DBs in local
storage.

If you are missing databases,
  mkdir -p scratch/db/
and the copy the databases you need from BECS:
  rsync -aiP amor:/scratch/networks/rmkujala/transit/db/<NAME> scratch/db/


The following functions exist:
  connect_*
  path_*
  *_gtfs
  *_gps

The connect_* functions return an sqlite3.Connection object, and the
path_* functions return the detected database path.  The *_gtfs paths
return GTFS data, and the argument 'name' is like hsl-2015-04-24, and
*_gps returns the GPS data, and the argument is the tag, like
'2015-03-01'.

The connection functions take a `mode` argument, a lot like the `open`
function.  The point here is that if you open with the 'r' mode (the
default), it will make an error if the database doesn't exist yet.  This
is important, because otherwise sqlite will create a new, empty database
and it won't work.
"""


import os
from os.path import join
import sqlite3

# The following lists possible paths for the "master" database copies.
# This is on scratch, or wherever is backed up and stable.
BASE_PATH = "./scratch/"
SEARCH_PATH = ['./scratch/', '../scratch/', '/scratch/networks/rmkujala/transit/']
for path in SEARCH_PATH:
    if os.path.exists(path):
        BASE_PATH = path
        break
else:
    # Print a warning about not finding the scratch dir.
    pass

GTFS_PATH = join(BASE_PATH, 'gtfs/')
DB_PATH = join(BASE_PATH, 'db/')

# The following is alternative search path for databases.  This is
# local disk.  If databases are found here, use them instead of the
# scratch/ ones which are defined in DB_PATH above.
DB_LOCAL = ['/local/cache/transit', ]

# Allow an environment variable to override the database path.  This
# is useful for testing purposes, but it has to be set when first
# importing.  Unit testing will need to use another option.
if 'TRANSIT_DB' in os.environ:
    DB_PATH = os.environ['TRANSIT_DB']
    DB_LOCAL = [os.environ['TRANSIT_DB'], ]

def find_file(fname, mode='r'):
    """Find a DB file and retun its path.

    This searches the local paths first, and returns the DB from there
    if it exists.  Then, it uses the SCRATCH path, from
    SEARCH_PATH/BASE_PATH variables.  BASE_PATH should be a link, or our
    shared "scratch" folder.
    """
    # First we check our local mirror paths.  These are faster to
    # access.
    for dir_ in DB_LOCAL:
        if os.path.exists(join(dir_, fname)):
            return join(dir_, fname)
    # If the DB_LOCAL env var is defined, force the use of local
    # paths, even if it does not exist.
    if 'DB_LOCAL' in os.environ:
        return join(DB_LOCAL[0], fname)
    # If this does not work, use the scratch dir path.
    path = join(DB_PATH, fname)
    if 'r' in mode and not os.path.exists(path):
        raise ValueError("Can not find file: %s.  See code/db.py for hints"%path)
    return path

# GTFS data and database connections
def raw_gtfs(name='hsl-2015-07-12'):
    """Return the base path to an extracted GTFS directory."""
    dirname = join(GTFS_PATH, name)
    if (not os.path.exists(join(dirname, 'stops.txt'))
        and len(os.listdir(dirname))==1
        and os.path.exists(join(dirname,os.listdir(dirname)[0], 'stops.txt'))):
        dirname = join(dirname, dirname,os.listdir(dirname)[0])
    # This is not a valid GTFS directory
    if not os.path.exists(join(dirname, 'stops.txt')):
        return None
    return dirname
def path_gtfs(name, mode='r'):
    """Return the path to a GTFS database"""
    if not name.endswith('.sqlite'):
        name = name+'.sqlite'
    return find_file(name, mode=mode)
def connect_gtfs(name='hsl-2015-07-12', fname=None, mode='r'):
    """Return a connection to a database"""
    if name == ':memory:':
        return name
    if fname is None:
        fname = path_gtfs(name, mode=mode)
    conn = sqlite3.connect(fname)
    post_connect(conn)
    return conn
connect = connect_gtfs

# HSL GPS data and database connections
def path_gps(name, mode='r'):
    if not name.endswith('.sqlite'):
        name = 'gps-'+name+'.sqlite'
    return find_file(name, mode=mode)
def connect_gps(name, fname=None, mode='r',
                gtfs=None):
    """Connect to GPS db.  Optionally, include a connection to GTFS, too.
    """
    if fname is None:
        fname = path_gps(name, mode=mode)
    conn = sqlite3.connect(fname)

    if gtfs:
        gtfs = path_gtfs(gtfs)
        conn.execute('ATTACH DATABASE ? AS gtfs', (gtfs, ))
        conn.commit()

        conn.execute('''CREATE TEMPORARY VIEW view_gps AS
                        SELECT gps.*, routes.*, stops.* FROM gps
                        LEFT JOIN gtfs.routes USING (route_id)
                        LEFT JOIN gtfs.stops USING (stop_id);''')
        conn.commit()
    post_connect(conn)
    return conn


# This was the old hackathon function.  Please now use connect_gps instead()
def get_db():
    raise NotImplementedError("Not implemented, use connect_gps() now.")



def post_connect(conn):
    """Standard initialitization after a connection is made"""
    # memory-mapped IO size, in bytes
    conn.execute('PRAGMA mmap_size = 1000000000;')
    # page cache size, in negative KiB.
    conn.execute('PRAGMA cache_size = -2000000;')

    from util import wgs84_distance
    conn.create_function("find_distance", 4, wgs84_distance)
