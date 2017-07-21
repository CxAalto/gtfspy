from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class RouteLoader(TableLoader):
    fname = 'routes.txt'
    table = 'routes'
    tabledef = '(route_I INTEGER PRIMARY KEY, ' \
               'route_id TEXT UNIQUE NOT NULL, ' \
               'agency_I INT, ' \
               'name TEXT, ' \
               'long_name TEXT, ' \
               'desc TEXT, ' \
               'type INT, ' \
               'url TEXT, ' \
               'color TEXT, ' \
               'text_color TEXT' \
               ')'
    extra_keys = ['agency_I', ]
    extra_values = ['(SELECT agency_I FROM agencies WHERE agency_id=:_agency_id )',
                    ]

    # route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url
    # 1001,HSL,1,Kauppatori - Kapyla,0,http://aikataulut.hsl.fi/linjat/fi/h1_1a.html
    def gen_rows(self, readers, prefixes):
        from gtfspy import extended_route_types
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print (row)
                yield dict(
                    route_id      = prefix + decode_six(row['route_id']),
                    _agency_id    = prefix + decode_six(row['agency_id']) if 'agency_id' in row else None,
                    name          = decode_six(row['route_short_name']),
                    long_name     = decode_six(row['route_long_name']),
                    desc          = decode_six(row['route_desc']) if 'route_desc' in row else None,
                    type          = extended_route_types.ROUTE_TYPE_CONVERSION[int(row['route_type'])],
                    url           = decode_six(row['route_url']) if 'route_url' in row else None,
                    color         = decode_six(row['route_color']) if 'route_color' in row else None,
                    text_color    = decode_six(row['route_text_color']) if 'route_text_color' in row else None,
                )

    @classmethod
    def index(cls, cur):
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_rid ON route (route_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_route_name ON routes (name)')