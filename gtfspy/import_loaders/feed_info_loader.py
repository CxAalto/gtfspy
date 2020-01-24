from gtfspy.gtfs import GTFS
from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class FeedInfoLoader(TableLoader):

    """feed_info.txt: various feed metadata"""

    fname = "feed_info.txt"
    table = "feed_info"
    tabledef = (
        "(feed_publisher_name TEXT, "
        "feed_publisher_url TEXT, "
        "feed_lang TEXT, "
        "feed_start_date TEXT, "
        "feed_end_date TEXT, "
        "feed_version TEXT, "
        "feed_id TEXT) "
    )

    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                # print row
                start = row["feed_start_date"] if "feed_start_date" in row else None
                end = row["feed_end_date"] if "feed_end_date" in row else None
                yield dict(
                    feed_publisher_name=decode_six(row["feed_publisher_name"])
                    if "feed_publisher_name" in row
                    else None,
                    feed_publisher_url=decode_six(row["feed_publisher_url"])
                    if "feed_publisher_url" in row
                    else None,
                    feed_lang=decode_six(row["feed_lang"]) if "feed_lang" in row else None,
                    feed_start_date="%s-%s-%s" % (start[:4], start[4:6], start[6:8])
                    if start
                    else None,
                    feed_end_date="%s-%s-%s" % (end[:4], end[4:6], end[6:8]) if end else None,
                    feed_version=decode_six(row["feed_version"]) if "feed_version" in row else None,
                    feed_id=prefix[:-1] if len(prefix) > 0 else prefix,
                )

    def post_import2(self, conn):
        # TODO! Something whould be done with this! Multiple feeds are possible, currently only selects one row for all feeds
        G = GTFS(conn)
        for name in [
            "feed_publisher_name",
            "feed_publisher_url",
            "feed_lang",
            "feed_start_date",
            "feed_end_date",
            "feed_version",
        ]:
            value = conn.execute("SELECT %s FROM feed_info" % name).fetchone()[0]
            if value:
                G.meta["feed_info_" + name] = value
