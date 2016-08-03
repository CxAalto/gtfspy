#!/usr/bin/env python

"""This file is a hack to do filtering of files.

For example, if you run "make import", it will import a *lot* of data.
That is too slow for quick testing.  So, instead you do "MAKE IMPORT
FILTER='-i philadelphia'", and it will only include (-i) data
containing philadelphia in the name.  Available operations are:

-i   include_pattern
-e   exclude_pattern
-dle date_less_or_equal   (YYYY-MM-DD)
-dge date_greater_or_equal

This whole thing is not very sophisticated, because it is a simple
evolution from make to Python.  This whole Makefile and filtering
system should eventually be re-written

"""

from sys import argv

def filter(*argv):
    """Process one filter command.

    argv: emulates argv of the program:
      argv[0]: not used
      argv[1]: file name
      argv[2]: command  (-i, -e, -dle, -dgt)
      argv[3]: argument (pattern to include/exclude, or date)

    """
    #import sys ; print >> sys.stderr, argv
    fname = argv[1]
    # If no operation: do nothing
    if len(argv) <= 2:
        return True
    # Parse command/argument
    cmd = argv[2]
    if len(argv) >= 4: arg = argv[3]
    # Do filtering based on command
    if cmd == '-i':
        if arg in fname:
            return True
        return False
    if cmd == '-e':
        if arg in fname:
            return False

    # Match a RE
    import re
    p = re.compile('(?P<region>[^/]+)/(?P<date>[0-9-]+)/(?P<part>[^/]+)')
    m = p.search(fname)
    if m is None:
        raise ValueError(argv)
    if cmd == '-dle':
        if m.group('date') <= arg:
            return True
        return False
    if cmd == '-dge':
        if m.group('date') >= arg:
            return True
        return False

    return True

def filter_all(*argv):
    """Run filter using every argument.

    This splits multiple filter commands into different calls to
    filter(), and returns True only if all are true.

    """
    fname = argv[1]
    cmd_and_args = zip(argv[2::2], argv[3::2])
    for cmd, arg in cmd_and_args:
        if not filter(None, fname, cmd, arg):
            return False
    return True

# Main script.  This takes individual filenames on the command line.
# There is some upper bound to this, so it will start failing
# eventually.  Then, we will have to figure out how Make can handle
# things...  We have to do it all in one process, since calling one
# python interperter per filtering process is too slow.
argv_options = argv[1]   # filter arguments.  Note it is *one* argument, spare-separated.
for fname in argv[2:]:
    if filter_all(None, fname, *argv_options.split(' ')):
        print fname
