
#cimport math
from libc.math cimport sin, cos, sqrt, atan2

cdef float TORADIANS = 3.141592653589793 / 180.
cdef float EARTH_RADIUS = 6378137.

def wgs84_distance(float lat1, float lon1, float lat2, float lon2):
    """Distance (in meters) between two points in WGS84 coord system."""
    cdef float dLat
    cdef float dLon
    cdef float a
    cdef float c
    cdef float d
    dLat = TORADIANS*(lat2 - lat1)
    dLon = TORADIANS*(lon2 - lon1)
    a = (sin(dLat / 2) * sin(dLat / 2) +
            cos(TORADIANS * lat1 ) * cos(TORADIANS * lat2) *
            sin(dLon / 2) * sin(dLon / 2))
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = EARTH_RADIUS * c
    return d

def wgs84_height(float meters):
    return meters/(EARTH_RADIUS * TORADIANS)

def wgs84_width(float meters, float lat):
    cdef float R2 = EARTH_RADIUS * cos(TORADIANS*lat)
    return meters/(R2 * TORADIANS)
