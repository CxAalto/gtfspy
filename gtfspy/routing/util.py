import time

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
        print('timeit: %r %2.2f sec (%r, %r) ' % (method.__name__, time_end-time_start, str(args)[:20], kw))
        return result

    return timed