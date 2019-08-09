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


def seconds_to_minutes(function):
    def wrapper(*args, **kwargs):
        func = function(*args, **kwargs)
        if func:
            return round(func / 60.0, 2)
    return wrapper


def if_df_empty_return_specified(target, value_to_return=[]):
    def deco(function):
        def inner(self, *args, **kwargs):
            if not getattr(self, target).empty:
                return function(self, *args, **kwargs)
            else:
                return value_to_return
        return inner
    return deco


def if_error_return_empty_list(apply_to_function):
    def wrapper(*args, **kwargs):
        try:
            func = apply_to_function(*args, **kwargs)
            return func
        except KeyError:
            return []
    return wrapper

