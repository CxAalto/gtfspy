from gtfspy.import_loaders.table_loader import TableLoader, decode_six


class ShapeLoader(TableLoader):
    fname = "shapes.txt"
    table = "shapes"
    tabledef = "(shape_id TEXT, lat REAL, lon REAL, seq INT, d INT)"

    # shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
    # 1001_20140811_1,60.167430,24.951684,1
    def gen_rows(self, readers, prefixes):
        for reader, prefix in zip(readers, prefixes):
            for row in reader:
                # print row
                yield dict(
                    shape_id=prefix + decode_six(row["shape_id"]),
                    lat=float(row["shape_pt_lat"]),
                    lon=float(row["shape_pt_lon"]),
                    seq=int(row["shape_pt_sequence"]),
                )

    @classmethod
    def index(cls, cur):
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_shapes_shid ON shapes (shape_id)')
        # cur.execute('CREATE INDEX IF NOT EXISTS idx_shapes_id_seq ON shapes (shape_I, seq)')
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shapes_id_seq ON shapes (shape_id, seq)")

    @classmethod
    def post_import(cls, cur):
        from gtfspy import shapes

        cur.execute("SELECT DISTINCT shape_id FROM shapes")
        shape_ids = tuple(x[0] for x in cur)

        # print "Renumbering sequences to start from 0 and Calculating shape cumulative distances"
        for shape_id in shape_ids:
            rows = cur.execute(
                "SELECT shape_id, seq " "FROM shapes " "WHERE shape_id=? " "ORDER BY seq",
                (shape_id,),
            ).fetchall()
            cur.executemany(
                "UPDATE shapes SET seq=? " "WHERE shape_id=? AND seq=?",
                ((i, shape_id, seq) for i, (shape_id, seq) in enumerate(rows)),
            )

        for shape_id in shape_ids:
            shape_points = shapes.get_shape_points(cur, shape_id)
            shapes.gen_cumulative_distances(shape_points)

            cur.executemany(
                "UPDATE shapes SET d=? " "WHERE shape_id=? AND seq=? ",
                ((pt["d"], shape_id, pt["seq"]) for pt in shape_points),
            )
