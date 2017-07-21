from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class TransfersLoader(TableLoader):
    """Loader to calculate transfer distances.

    transfer_type, from GTFS spec:
      0/null: recommended transfer point
      1: timed transfer
      2: minimum amount of time
      3: transfers not possible

    """
    # This loader is special.  calc_transfers creates the table there,
    # too.  We put a tabledef here so that copy() will work.
    fname = 'transfers.txt'
    table = 'transfers'
    # TODO: this is copy-pasted from calc_transfers.
    tabledef = ('(from_stop_I INT, '
                'to_stop_I INT, '
                'transfer_type INT, '
                'min_transfer_time INT'
                ')')
    extra_keys = ['from_stop_I',
                  'to_stop_I',
                  ]
    extra_values = ['(SELECT stop_I FROM stops WHERE stop_id=:_from_stop_id)',
                    '(SELECT stop_I FROM stops WHERE stop_id=:_to_stop_id)',
                    ]

    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                #print row
                yield dict(
                    _from_stop_id     = prefix + decode_six(row['from_stop_id']).strip(),
                    _to_stop_id       = prefix + decode_six(row['to_stop_id']).strip(),
                    transfer_type     = int(row['transfer_type']),
                    min_transfer_time = int(row['min_transfer_time'])
                                        if ('min_transfer_time' in row
                                        and (row.get('min_transfer_time').strip()) )
                                        else None
            )