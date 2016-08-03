import frequencies as freqs

def test_get_stop_freqs():
    """
    Test the get_stop_freqs function in frequencies.py

    freq_list = data['trips']
    freq = freq_list[0] # there should be at least one freq for a valid query
    # assert data is of correct type
    type(freq['name']) : str/unicode
    type(freq['lat']) : list
    type(freq['lon']) : list
    type(freq['freq']) : list
    """
    hour = 8
    day = '2015-08-08'
    d = freqs.get_stop_freqs('w2015', 'w', hour)
    flist = d['all_stop_freqs']
    assert len(flist) > 0
    f = flist[0]

    ftypes = [str, float, float, int]
    keys = ['name', 'lat', 'lon', 'freq']
    _assert_types(f, keys, ftypes)

    for f in flist:
        assert f['freq'] >= 0

def test_diff_stop_freqs():
    hour1 = 8
    hour2 = 9
    d = freqs.diff_stop_freqs('w2015', 'w', hour1, 'w2015', 'w', hour2)

    flist = d['all_stop_freqs']
    assert len(flist) > 0
    f = flist[0]

    ftypes = [str, float, float, int]
    keys = ['name', 'lat', 'lon', 'freq']
    _assert_types(f, keys, ftypes)




def test_get_pair_freqs():
    d = freqs.get_pair_freqs('s2015', 'm', 8)
    assert type(d) == dict
    pfreqs = d['all_pair_freqs']
    assert len(pfreqs) > 0
    f = pfreqs[0]


    ftypes = [str, list, list, int]
    keys = ['name', 'lats', 'lons', 'freq']
    _assert_types(f, keys, ftypes)

    for f in pfreqs:
        assert f['freq'] >= 0




def test_diff_pair_freqs():
    """ Just testing out schema """
    d = freqs.diff_pair_freqs('s2015', 'm', 8, 'w2015', 'm', 8, with_shapes=True)
    assert type(d) == dict
    pfreqs = d['all_pair_freqs']
    assert len(pfreqs) > 0
    f = pfreqs[0]

    ftypes = [str, list, list, int]
    keys = ['name', 'lats', 'lons', 'freq']

    _assert_types(f, keys, ftypes)




def test_diff_create():
    """
    Test diff create basic functionality.
    """

    da = {'key':{'data': 1, 'freq': 2}}
    db = {'key':{'data': 2, 'freq': 1}}

    # db-da
    diffd = freqs.diff_create(da, db)
    assert len(diffd) == 1
    assert diffd['key']['freq'] == -1


def test_get_metainfo():
    daynumber, dbname, monday, day = freqs.get_metainfo('w2015', 'm')
    try:
        a, b, c = freqs.get_metainfo('nonexistingseason', 'm')
    except Exception as e:
        assert isinstance(e, UnboundLocalError), 'error should be raised if '


def _assert_types(d, keys, val_types):
    """
    Assert that the types of a dictionary are correct.
    """
    for key, val_type in zip(keys, val_types):
        if val_type == str:
            assert isinstance(d[key], val_type) or isinstance(d[key], unicode), str(type(d[key])) + ", should be " + str(val_type)
            continue
        assert isinstance(d[key], val_type), str(type(d[key])) + ", should be " + str(val_type)


