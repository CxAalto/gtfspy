import delay_viz
import types

def test_delay_viz_schema():
    """
    Tests that delay viz returns data in the format:

    trip_list = data['trips']
    trip = trip_list[0] # there should be at least one trip for a valid query
    # assert data is of correct type
    type(trip['name']) : str/unicode
    type(trip['lats']) : list
    type(trip['delays']) : list
    type(trip['times']) : list
    type(trip['lons']) : list
    type(trip['id']) : str/unicode
    """
    data = delay_viz.get_trips(1425183840, 1425183840+3600*1)
    assert data.has_key('trips'), "data has no key trips"
    trips = data['trips']

    n_trips = len(trips)
    assert n_trips > 0
    trip = trips[0]
    keys = ['name', 'lats', 'delays', 'times', 'lons', 'id']
    value_types = [str, list, list, list, list, str]

    for key, t in zip(keys, value_types):
        assert trip.has_key(key), "trip does not have key " + key
        if t == str:
            assert isinstance(trip[key], types.StringType) or isinstance(trip[key], unicode), "incorrect type: trip["+key+"] should be of type 'str' or 'unicode', but is " + str(type(trip[key]))
        else:
            assert isinstance(trip[key], t), str(type(trip[key])) + ","  + str(t)
    ids = set([trip['id'] for trip in trips])
    assert len(ids) == n_trips, 'Some trip ids are not unique!'



if __name__ == "__main__":
    test_delay_viz_schema()
