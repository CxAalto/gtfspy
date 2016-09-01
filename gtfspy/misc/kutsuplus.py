import sqlite3


class KutsuplusTraces:

    def __init__(self, dbfname="scratch/web/kutsuplus/kutsuplus-scrape-small.sqlite"):
        self.dbfname = dbfname
        self.conn = sqlite3.connect(self.dbfname)

    def get_span(self):
        cur = self.conn.cursor()
        query = "SELECT min(time_ut), max(time_ut) from traces"
        row = cur.execute(query).fetchone()
        return row[0], row[1]

    def get_trips(self, start_ut, end_ut):
        cur = self.conn.cursor()
        # get distinct lineRefs
        params = [start_ut, end_ut]
        query = "SELECT distinct(lineRef) FROM traces WHERE time_ut >= ? and time_ut <= ?"
        rows = cur.execute(query, params)
        kutsu_ids = [row[0] for row in rows]
        trips = []
        for kutsu_id in kutsu_ids:
            query = "SELECT time_ut, lat, lon FROM traces WHERE time_ut >= ? and time_ut <= ? and lineRef = ?"
            params = [start_ut, end_ut, kutsu_id]
            rows = cur.execute(query, params)
            lats = []
            lons = []
            times = []
            for row in rows:
                time, lat, lon = row
                lats.append(lat)
                lons.append(lon)
                times.append(time)
            trip = {
                "name" : kutsu_id,
                "times" : times,
                "lats" : lats,
                "lons" : lons,
                "route_type": "3", # gtfs code for a bus
            }
            trips.append(trip)
        return {"trips":trips}


if __name__ == "__main__":
    kt = KutsuplusTraces("../scratch/web/kutsuplus/kutsuplus-scrape-small.sqlite")
    ktmin, ktmax = kt.get_span()
    trips = kt.get_trips(ktmin, ktmin+1000) # 1000 seconds interval
    print trips
    ktmin, ktmax = 1441416839, 1441417139
    print kt.get_trips(ktmin, ktmax)




