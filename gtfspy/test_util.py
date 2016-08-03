import util

def approxeq(a, b):
    return abs(a-b) / float(abs(a+b))  <  1e-2

def test_ut_to_utc_datetime_str():
    time_ut = 1438300800+24299
    assert util.ut_to_utc_datetime_str(time_ut) == "Jul 31 2015 06:44:59"

def test_wgs84_width():
    lat = 60.4192161560059
    lon = 25.3302955627441
    # Test going 100m east
    width = util.wgs84_width(100, lat)
    lon2 = lon+width
    d = util.wgs84_distance(lat, lon, lat, lon2)
    print d
    assert approxeq(d, 100)
    # Test going 100m south
    height = util.wgs84_height(100)
    lat2 = lat + height
    d = util.wgs84_distance(lat, lon, lat2, lon)
    print d
    assert approxeq(d, 100)

def test_day_seconds_to_str_time():
    str_time = util.day_seconds_to_str_time(25*3600 + 59*60 + 10)
    assert str_time == "25:59:10", str_time
    str_time = util.day_seconds_to_str_time(0)
    assert str_time == "00:00:00", str_time

