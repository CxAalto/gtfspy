import json
import sqlite3
from os import listdir
from os.path import isfile, join
import os, errno
import re



def read_into_db(scrape_fname, cursor):
    """
    Reads hsl live (Siri) feed data, and records all events starting with 
    a 'k' or a 'K' into a database.

    These codes are assumed to correspond to the Kutsuplus vans.
    (Would need to check this somehow, I think.)


    """
    with open(scrape_fname, 'r') as f:
        for line in f:
            try:
                js = json.loads(line) # ValueErrors may arise
                siri = js['Siri']
                serviceDelivery = siri['ServiceDelivery']
                responceTimeStamp = serviceDelivery['ResponseTimestamp'] # integer
                data = serviceDelivery['VehicleMonitoringDelivery'][0]['VehicleActivity']
                for datum in data:
                    time = datum['RecordedAtTime']
                    mvh = datum['MonitoredVehicleJourney']
                    lat = mvh['VehicleLocation']['Latitude']
                    lon = mvh['VehicleLocation']['Longitude']
                    lref = mvh['LineRef']['value']

                    # filter: take only those that start with 'k' or 'K' 
                    # these are assumed to correspond to Kutsuplus vans
                    if lref[0].lower() == 'k':
                        assert lat < 65 and lat > 55 and lon < 30 and lon > 10, "weird coordinates!"
                        execute_str = "INSERT INTO traces VALUES (%d, %f, %f, '%s')" % (time/1000, lat, lon, lref) 
                        c.execute(execute_str)
            except Exception as e:
                # Check that nothing unknown is not going on: 
                # someties there is missing data etc.:
                knownError = ((line[:2] == "==") or line == "\n")
                if not knownError:
                    print "Found some other error"
                    print e

                # assert knownError, "see kutsuplus_import.log for more details"






if __name__ == "__main__":
    small = False
    if small:
        print "small!"
        dbfname = "scratch/db/kutsuplus-scrape-small.sqlite"
    else:
        dbfname = "scratch/db/kutsuplus-scrape.sqlite"

    # remove any old instances of the databases
    # (dropping tables can take time)

    try:
        os.remove(dbfname)
        print "removed old file"
    except OSError:
        print "no file was present"
        pass

    conn = sqlite3.connect(dbfname)
    c = conn.cursor()
    c.execute('''CREATE TABLE traces
             (time_ut integer, lat real, lon real, lineRef text)''')

    scrape_path = "scratch/hsl-scrape-1"

    files = [f for f in listdir(scrape_path) if isfile(join(scrape_path, f))]
    regex = re.compile('^log-[0-9]{4}-[0-9]{2}-[0-9]{2}(|-hammer|-thor)\.txt*')

    # for the meaning of this regex, see https://regex101.com/#python
    files = filter(regex.match, files)

    print len(files)

    for i, fname in enumerate(files):
        fullpath = join(scrape_path, fname)
        print str(i) + "/" + str(len(files)) + " :" +  fullpath
        if small and i > 2:
            break
        read_into_db(fullpath, c)
        conn.commit()

    conn.close()

