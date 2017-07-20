from gtfspy.import_loaders.table_loader import TableLoader


class MetadataLoader(TableLoader):
    """Table to be used for any type of metadata"""
    fname = None
    table = 'metadata'
    tabledef = '(key TEXT UNIQUE NOT NULL, value BLOB, value2 BLOB)'

    @classmethod
    def index(cls, cur):
        cur.execute('CREATE INDEX IF NOT EXISTS idx_metadata_name '
                    'ON metadata (key)')

    @classmethod
    def copy(cls, conn, **where):
        pass