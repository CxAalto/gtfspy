import os
import time
from datetime import datetime

from gtfspy.import_loaders.table_loader import TableLoader, decode_six
from gtfspy.util import set_process_timezone


class AgencyLoader(TableLoader):
    fname = "agency.txt"
    table = "agencies"
    tabledef = (
        "(agency_I INTEGER PRIMARY KEY, agency_id TEXT UNIQUE NOT NULL, "
        "name TEXT, url TEXT, timezone TEXT, lang TEXT, phone TEXT)"
    )

    # shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
    # 1001_20140811_1,60.167430,24.951684,1
    def gen_rows(self, readers, prefixes):

        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                yield dict(
                    agency_id=prefix + decode_six(row.get("agency_id", "1")),
                    name=decode_six(row["agency_name"]),
                    timezone=decode_six(row["agency_timezone"]),
                    url=decode_six(row["agency_url"]),
                    lang=decode_six(row["agency_lang"]) if "agency_lang" in row else None,
                    phone=decode_six(row["agency_phone"]) if "agency_phone" in row else None,
                )

    def post_import(self, cur):
        TZs = cur.execute("SELECT DISTINCT timezone FROM agencies").fetchall()
        if len(TZs) == 0:
            raise ValueError("Error: no timezones defined in sources: %s" % self.gtfs_sources)
        elif len(TZs) > 1:
            first_tz = TZs[0][0]
            import pytz

            for tz in TZs[1:]:
                generic_date = datetime(2009, 9, 1)
                ftz = pytz.timezone(first_tz).utcoffset(generic_date, is_dst=True)
                ctz = pytz.timezone(tz[0]).utcoffset(generic_date, is_dst=True)
                if not str(ftz) == str(ctz):
                    raise ValueError(
                        "Error: multiple timezones defined in sources:: %s" % self.gtfs_sources
                    )
        TZ = TZs[0][0]
        set_process_timezone(TZ)

    def index(self, cur):
        pass
