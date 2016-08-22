from subprocess import call, Popen
import json
import numpy as np
import time
import time

import requests

class OSMR_router:

    def __init__(self, pbf_file, lua_profile, port, contraction_frac, extracted=False, prepared=False):
        """
        Initialize the object, initiate routing, and fire up the server.

        Parameters
        ----------
        pbf_file : str
            Open street map data (extract)
        lua_profile : str
            Profile file for setting up the routing
        port: int
            Which port should the server use.
        contraction_frac : float
            What fraction of nodes are contracted in the contraction hierarchies algorithm. (in osrm-prepare) OSMR algorithm.
        """
        self.pbf_file = pbf_file
        self.base = pbf_file.split(".osm.pbf")[0]
        self.port = port
        self.profile = lua_profile
        self.contraction_frac = contraction_frac
        self.server_process = None
        # initialize server
        print "Launching up the server"
        if not prepared:
            if not extracted:
                self._extract()
            self._prepare()
        self._routed()
        print "Server up"

    def _extract(self):
        """
        Extract essential information from the pbf file.
        """
        cmd = ["osrm-extract", self.pbf_file, "--profile", self.profile]
        call(cmd)

    def _prepare(self):
        """
        Prepare routing (does contraction hierarchies etc., I guess).
        """
        cmd = ["osrm-prepare", self.base+".osrm", "--profile", self.profile, \
                "-k", str(self.contraction_frac)]
        call(cmd)

    def _routed(self):
        """
        Fire up the server
        """
        cmd = ["osrm-routed", self.base+".osrm",  "--port",  str(self.port), "--max-table-size",  str(100)]
        self.server_process = Popen(cmd)
        while True:
            if self.ready():
                print "ready!"
                return True
            time.sleep(0.5)

    def ready(self):
        """
        Test whether the server is ready to rock'n roll?

        Returns
        -------
        ready: bool
            if ready=True, the server should be up and running.
        """
        try:
            requests.get("http://localhost:"+str(self.port))
            return True
        except Exception as e:
            print e
            return False


    def get_travel_times(self, locs):
        """
        Parameters
        ----------
        locs: list
            list of (lat, lon) tuples

        Returns
        -------
        travel_times: array
            A 2 dimensional array of travel times in a table.
        """
        print "Fixed command, but works"
        query = "/viaroute?loc=60.171263,24.956605&loc=60.165197,24.952593&instructions=false&alt=false"
        r = requests.get("http://localhost:"+str(self.port)+query)
        print r.json()

    def get_distance_table(locs):
        print "Fixed command, but works"

        query = "/table?"
        for lat, lon in locs:
            query += "loc="+str(lat)+","+str(lon)+"&"
        query = query[:-1]
        r = requests.get("http://localhost:"+str(self.port)+query)
        print r.json()


    def get_dist_and_time(self, loc1, loc2):
        """
        Parameters
        ----------
        loc1: tuple
            lat, lon
        loc2: tuple
            lat, lon

        Return
        ------
        dist: float
            distance in meters
        time: float
            estimated travel time in seconds
            (depends on the profile)
        """
        query = "/viaroute?loc=%f,%f&loc=%f,%f&instructions=false&alt=false" %(loc1[0], loc1[1], loc2[0], loc2[1])
        r = requests.get("http://localhost:"+str(self.port)+query).json()
        time = r['route_summary']['total_time']
        dist = r['route_summary']['total_distance']
        return dist, time

    def _end_routed(self):
        self.server_process.terminate()

    def clean(self):
        """
        Remove all created files
        """
        cmd = ["rm", self.base+".osrm*"]
        call(cmd)


if __name__ == "__main__":
    try:
        pbf= "/home/rmkujala/work/osrm-test/helsinki_finland.osm.pbf"
        profile = "/home/rmkujala/work/osrm-test/osrm-backend/profiles/foot.lua"
        port = 5009
        s = time.time()
        router = OSMR_router(pbf, profile, port, 1.0, prepared=True)
        e = time.time()
        print s, e, e-s
        print "router ready?", router.ready()

        start = time.time()
        for i in range(1000):
            rands = np.random.randn(4)/100
            loc1 = 60.171263+rands[0],24.956605+rands[1]
            loc2 = 60.165197+rands[2],24.952593+rands[3]
            a = router.get_dist_and_time(loc1, loc2)
        end = time.time()
        print start, end, end-start
    except:
        pass
    if router.ready():
        router._end_routed()



